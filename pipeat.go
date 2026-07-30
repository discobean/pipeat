package pipeat

// Attempts to provide similar funcationality to io.Pipe except supporting
// ReaderAt and WriterAt. It uses a temp file as a buffer in order to implement
// the interfaces as well as allow for asyncronous writes.

// Author: John Eikenberry <jae@zhar.net>
// License: CC0 <http://creativecommons.org/publicdomain/zero/1.0/>

// This is the discobean/pipeat fork (github.com/discobean/pipeat). On top of
// upstream it hardens the close/error/accounting semantics that a file server
// sitting between an SFTP client and object storage depends on:
//
//   - ReadAt never reads past the contiguous written frontier (endln), so an
//     unwritten hole can never be returned as fabricated zero bytes.
//   - A reader asking for exactly the bytes already written completes without
//     waiting for the writer to close (upstream had an off-by-one that made
//     every exact-boundary read stall until eow).
//   - In synchronous mode the writer's Close/CloseWithError returns the
//     reader's terminal error (e.g. a failed upstream upload) instead of
//     swallowing it; a clean Close with unfilled gaps returns ErrUnfilledGap.
//   - Write/WriteAt check both ends' closed state under the file lock, so no
//     write can succeed after Close has returned (close is linearized).
//   - Partial writes are accounted (n is returned and published) and the real
//     I/O error is propagated instead of being masked as io.EOF.
//   - The write-ahead span queue is a proper interval union: overlapping and
//     adjacent spans merge, and spans below the frontier are dropped, so the
//     frontier cannot stall behind data that is already in the file.
//   - Write() tracks its own sequential offset (shared with WriteAt's
//     accounting) instead of the file cursor, so mixing Write and WriteAt is
//     consistent; Read() serializes concurrent callers instead of racing on
//     the shared read offset.
//   - Close on either end is safe and idempotent, including from concurrent
//     panic-recovery paths; the first error wins and is sticky.
//   - Constructor error paths close and remove the temp file instead of
//     leaking the descriptor and name.

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)

// ErrUnfilledGap is returned (and surfaced to the reader) when the writer is
// closed cleanly while one or more regions before the highest written offset
// were never written. Reading past the contiguous frontier would fabricate
// data, so the pipe fails closed instead: the transfer is incomplete.
var ErrUnfilledGap = errors.New("pipeat: writer closed with unwritten gap")

var (
	errNegativeOffset = errors.New("pipeat: negative offset")
	errOffsetOverflow = errors.New("pipeat: offset overflow")
)

// Used to track write ahead areas of a file. That is, areas where there is a
// gap in the file data earlier in the file. Possible with concurrent writes.
type span struct {
	start, end int64
}

type spans []span

func (c spans) Len() int           { return len(c) }
func (c spans) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c spans) Less(i, j int) bool { return c[i].start < c[j].start }

// The wrapper around the underlying temp file.
type pipeFile struct {
	*os.File
	fileLock  sync.RWMutex
	rCond     sync.Cond    // used to signal readers from writers
	wCond     sync.Cond    // used to signal writers from readers in sync mode
	dataLock  sync.RWMutex // serialize access to meta data (below)
	syncMode  bool         // if true the writer is only allowed to write until the reader requested
	endln     int64        // contiguous written frontier: bytes [0,endln) are all present
	ahead     spans        // sorted, coalesced spans written beyond the frontier
	writeroff int64        // file offset allowed for the writer in sync mode
	readed    int64        // size readed as bytes, useful for stats
	written   int64        // size written as bytes, useful for stats
	rerr      error
	werr      error
	eow       chan struct{} // end of writing
	eor       chan struct{} // end of reading

	fileCloseOnce sync.Once // the reader owns the fd; close it exactly once
	fileCloseErr  error
}

func newPipeFile(dirPath string) (*pipeFile, error) {
	file, err := os.CreateTemp(dirPath, "pipefile")
	if err != nil {
		return nil, err
	}
	name := file.Name()
	file, err = unlinkFile(file)
	if err != nil {
		// Don't leak the descriptor or the on-disk name on a failed
		// unlink/reopen; report the original error.
		if file != nil {
			file.Close()
		}
		os.Remove(name)
		return nil, err
	}
	f := &pipeFile{File: file,
		eow: make(chan struct{}),
		eor: make(chan struct{})}
	f.rCond.L = f.dataLock.RLocker() // Readers cond locker
	f.wCond.L = f.dataLock.RLocker() // Writers cond locker
	return f, nil
}

// waitForReadable waits until every byte below the requested (exclusive) end
// offset is available to read. We can stop waiting if the writer ended or if
// the reader is closed. The reader can be closed using Close() or
// CloseWithError(); we don't want to wait on a closed reader.
func (f *pipeFile) waitForReadable(end int64) {
	f.dataLock.RLock()
	defer f.dataLock.RUnlock()

	for end > f.endln {
		select {
		case <-f.eow:
			trace("eow")
			return
		case <-f.eor:
			trace("eor")
			return
		default:
			f.rCond.Wait()
		}
	}
}

func (f *pipeFile) updateReadedBytes(n int) {
	f.dataLock.Lock()
	defer f.dataLock.Unlock()
	f.readed += int64(n)
}

func (f *pipeFile) readerror() error {
	f.dataLock.RLock()
	defer f.dataLock.RUnlock()
	return f.rerr
}

func (f *pipeFile) setReaderror(err error) {
	f.dataLock.Lock()
	defer f.dataLock.Unlock()

	if f.rerr == nil {
		f.rerr = err
		close(f.eor)
	}
	f.rCond.Broadcast()
	f.wCond.Broadcast()
}

// waitForWritable don't allow to write more than the reader requested.
// If the pipe is not in sync mode it does nothing.
// We can stop waiting it the reader need more data or if the reader or
// the writer are closed
func (f *pipeFile) waitForWritable() {
	if !f.syncMode {
		return
	}
	f.dataLock.RLock()
	defer f.dataLock.RUnlock()

	for f.endln > f.writeroff {
		select {
		case <-f.eow:
			trace("eow")
			return
		case <-f.eor:
			trace("eor")
			return
		default:
			f.wCond.Wait()
		}
	}
}

func (f *pipeFile) updateWrittenBytes(n int) {
	f.dataLock.Lock()
	defer f.dataLock.Unlock()
	f.written += int64(n)
}

func (f *pipeFile) writeerror() error {
	f.dataLock.RLock()
	defer f.dataLock.RUnlock()
	return f.werr
}

// set the new allowed write offset and signal the writers.
// Do nothing if not in sync mode
func (f *pipeFile) setWriteoff(off int64) {
	if !f.syncMode {
		return
	}
	f.dataLock.Lock()
	defer f.dataLock.Unlock()
	if off > f.writeroff {
		f.writeroff = off
		f.wCond.Broadcast()
	}
}

// publishSpan records that bytes [start,end) have been written, advancing the
// contiguous frontier (endln) when possible and otherwise keeping the ahead
// queue as a sorted union of disjoint intervals. Callers must NOT hold
// dataLock.
func (f *pipeFile) publishSpan(start, end int64) {
	f.dataLock.Lock()
	defer f.dataLock.Unlock()

	if end <= f.endln {
		return // rewrite wholly below the frontier: nothing new
	}
	if start <= f.endln {
		// extends the frontier, possibly absorbing queued spans
		f.endln = end
		i := 0
		for ; i < len(f.ahead); i++ {
			s := f.ahead[i]
			if s.start > f.endln {
				break
			}
			if s.end > f.endln {
				f.endln = s.end
			}
		}
		if i > 0 { // clean up ahead queue
			f.ahead = append(f.ahead[:0], f.ahead[i:]...)
		}
		f.rCond.Broadcast()
		return
	}
	// write-ahead region disjoint from the frontier: insert, keeping the
	// queue sorted and coalescing overlapping/adjacent spans so it cannot
	// grow without bound under rewrites.
	f.ahead = append(f.ahead, span{start, end})
	sort.Sort(f.ahead)
	merged := f.ahead[:1]
	for _, s := range f.ahead[1:] {
		last := &merged[len(merged)-1]
		if s.start <= last.end {
			if s.end > last.end {
				last.end = s.end
			}
		} else {
			merged = append(merged, s)
		}
	}
	f.ahead = merged
	// trace(f.ahead)
}

// PipeWriterAt is the io.WriterAt side of pipe.
type PipeWriterAt struct {
	f           *pipeFile
	asyncWriter bool

	writeMu  sync.Mutex // serializes Write() and guards writeoff
	writeoff int64      // sequential offset for Write(); independent of the file cursor
}

// PipeReaderAt is the io.ReaderAt side of pipe.
type PipeReaderAt struct {
	f *pipeFile

	readMu  sync.Mutex // serializes Read() and guards readoff
	readoff int64      // offset where Read() last read
}

// Pipe creates an asynchronous file based pipe. It can be used to connect code
// expecting an io.ReaderAt with code expecting an io.WriterAt. Writes all go
// to an unlinked temporary file, reads start up as the file gets written up to
// their area. It is safe to call multiple ReadAt and WriteAt in parallel with
// each other.
func Pipe() (*PipeReaderAt, *PipeWriterAt, error) {
	return PipeInDir("")
}

// PipeInDir just like Pipe but the temporary file is created inside the specified
// directory
func PipeInDir(dirPath string) (*PipeReaderAt, *PipeWriterAt, error) {
	return newPipe(dirPath, false)
}

// AsyncWriterPipe is just like Pipe but the writer is allowed to close before
// the reader is finished. Whereas in Pipe the writer blocks until the reader
// is done.
func AsyncWriterPipe() (*PipeReaderAt, *PipeWriterAt, error) {
	return AsyncWriterPipeInDir("")
}

// AsyncWriterPipeInDir is just like AsyncWriterPipe but the temporary file is created
// inside the specified directory
func AsyncWriterPipeInDir(dirPath string) (*PipeReaderAt, *PipeWriterAt, error) {
	return newPipe(dirPath, true)
}

func newPipe(dirPath string, asyncWriter bool) (*PipeReaderAt, *PipeWriterAt, error) {
	fp, err := newPipeFile(dirPath)
	if err != nil {
		return nil, nil, err
	}
	fp.syncMode = !asyncWriter
	return &PipeReaderAt{f: fp}, &PipeWriterAt{f: fp, asyncWriter: asyncWriter}, nil
}

// ReadAt implements the standard ReaderAt interface. It blocks until the full
// requested range has been written (or an end closes), and never reads beyond
// the contiguous written frontier — an unwritten gap is never returned as
// data. You can call it from multiple threads.
func (r *PipeReaderAt) ReadAt(p []byte, off int64) (int, error) {
	trace("readat", off)

	if off < 0 {
		return 0, errNegativeOffset
	}
	end := off + int64(len(p))
	if end < off {
		return 0, errOffsetOverflow
	}
	if len(p) == 0 {
		if err := r.f.readerror(); err != nil {
			return 0, err
		}
		return 0, nil
	}

	r.f.setWriteoff(end)
	r.f.waitForReadable(end)

	r.f.fileLock.RLock()
	defer r.f.fileLock.RUnlock()

	if err := r.f.readerror(); err != nil {
		trace("end readat(1):", off, 0, err)
		return 0, err
	}

	// Snapshot the frontier and writer state; endln only ever grows, and
	// werr is sticky, so a stale snapshot is always conservative.
	r.f.dataLock.RLock()
	endln := r.f.endln
	werr := r.f.werr
	r.f.dataLock.RUnlock()

	if off >= endln {
		// Nothing logically readable at this offset. We can only get here
		// once the writer has closed (waitForReadable otherwise blocks).
		if werr != nil {
			return 0, werr
		}
		return 0, io.EOF
	}

	// Clamp the physical read to the frontier so holes are unreachable.
	limit := int64(len(p))
	if avail := endln - off; avail < limit {
		limit = avail
	}
	n, err := r.f.File.ReadAt(p[:limit], off)
	r.f.updateReadedBytes(n)
	if err != nil {
		// Translate only a genuine end-of-data into the writer's terminal
		// error; real storage errors (EIO etc.) must be preserved.
		if errors.Is(err, io.EOF) && werr != nil {
			err = werr
		}
	} else if int64(n) < int64(len(p)) {
		// Short logical read: only possible once the writer has closed.
		err = werr
		if err == nil {
			err = io.EOF
		}
	}
	trace("end readat(2):", off, n, err)
	return n, err
}

// Read provides a standard io.Reader interface. Concurrent Read calls are
// serialized; each consumes the stream in order.
func (r *PipeReaderAt) Read(p []byte) (int, error) {
	trace("read", len(p))
	r.readMu.Lock()
	defer r.readMu.Unlock()
	n, err := r.ReadAt(p, r.readoff)
	r.readoff += int64(n)
	trace("end read", n, err)
	return n, err
}

// GetReadedBytes returns the bytes readed
func (r *PipeReaderAt) GetReadedBytes() int64 {
	r.f.dataLock.RLock()
	defer r.f.dataLock.RUnlock()
	return r.f.readed
}

// Close will Close the temp file and subsequent writes or reads will return an
// error. Close is idempotent: extra calls return the first close's result.
func (r *PipeReaderAt) Close() error {
	return r.CloseWithError(nil)
}

// CloseWithError sets error and otherwise behaves like Close. The first
// error set wins and is sticky; later calls do not overwrite it.
func (r *PipeReaderAt) CloseWithError(err error) error {
	if err == nil {
		err = io.EOF
	}
	r.f.fileLock.Lock()
	defer r.f.fileLock.Unlock()
	r.f.setReaderror(err)
	r.f.fileCloseOnce.Do(func() { r.f.fileCloseErr = r.f.File.Close() })
	return r.f.fileCloseErr
}

// WriteAt implements the standard WriterAt interface. It will write to the
// temp file without blocking. You can call it from multiple threads.
func (w *PipeWriterAt) WriteAt(p []byte, off int64) (int, error) {
	//trace("writeat: ", string(p), off)
	//defer trace("wrote: ", string(p), off)

	if off < 0 {
		return 0, errNegativeOffset
	}
	if off+int64(len(p)) < off {
		return 0, errOffsetOverflow
	}

	w.f.waitForWritable()

	w.f.fileLock.RLock()
	defer w.f.fileLock.RUnlock()

	// Both checks happen under fileLock, and both closes take fileLock
	// exclusively, so no write can start after a Close has returned.
	if err := w.f.writeerror(); err != nil {
		return 0, err
	}
	if err := w.f.readerror(); err != nil {
		return 0, err
	}

	n, err := w.f.File.WriteAt(p, off)
	w.f.updateWrittenBytes(n)
	if n > 0 {
		// Account whatever actually landed, even on a partial write, so the
		// logical file never diverges from the physical one.
		w.f.publishSpan(off, off+int64(n))
	}
	// Real errors (e.g. ENOSPC) are returned as-is with the true n.
	return n, err
}

// Write provides a standard io.Writer interface. It maintains its own
// sequential offset (starting at 0), so it composes correctly with WriteAt
// and concurrent Write calls are serialized.
func (w *PipeWriterAt) Write(p []byte) (int, error) {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	n, err := w.WriteAt(p, w.writeoff)
	w.writeoff += int64(n)
	return n, err
}

// GetWrittenBytes returns the bytes written
func (w *PipeWriterAt) GetWrittenBytes() int64 {
	w.f.dataLock.RLock()
	defer w.f.dataLock.RUnlock()
	return w.f.written
}

// Close on the writer will let the reader know that writing is complete. Once
// the reader catches up it will continue to return 0 bytes and an EOF error.
//
// In synchronous mode (Pipe) Close waits for the reader to finish and returns
// the reader's terminal error if it closed with one — so a failed downstream
// consumer (e.g. an upload that did not complete) surfaces here instead of
// being silently dropped. A clean close while unwritten gaps remain returns
// ErrUnfilledGap.
func (w *PipeWriterAt) Close() error {
	return w.CloseWithError(nil)
}

// CloseWithError sets the error and otherwise behaves like Close. The first
// close's error wins and is sticky.
//
// Clean-close detection is by identity against the io.EOF sentinel the pipe
// installs itself — NOT errors.Is — so a real failure that merely wraps
// io.EOF (e.g. "upload failed: EOF") is still reported as a failure. (A
// caller passing literal io.EOF is treated as a clean close; that is what
// io.EOF means.)
func (w *PipeWriterAt) CloseWithError(err error) error {
	// Take fileLock exclusively so close linearizes with in-flight
	// Write/WriteAt calls: once we return, no write can succeed.
	// Literal io.EOF from the caller means "clean close" — so it must go
	// through gap detection exactly like Close(), not bypass it.
	clean := err == nil || err == io.EOF

	w.f.fileLock.Lock()
	w.f.dataLock.Lock()
	installed := false // did THIS call set the terminal state?
	if w.f.werr == nil {
		installed = true
		werr := err
		if clean {
			werr = io.EOF
			if len(w.f.ahead) > 0 {
				// A clean close with data still disjoint from the frontier
				// means bytes below the high-water mark were never written.
				// Fail closed: the reader must not treat this as a complete
				// stream.
				werr = ErrUnfilledGap
			}
		}
		w.f.werr = werr
		close(w.f.eow)
	}
	final := w.f.werr
	w.f.rCond.Broadcast()
	w.f.wCond.Broadcast()
	w.f.dataLock.Unlock()
	w.f.fileLock.Unlock()

	// write is closed at this point
	if !w.asyncWriter {
		// Identity comparison on purpose (see doc comment above).
		if rerr := w.WaitForReader(); rerr != nil && rerr != io.EOF {
			return rerr
		}
	}
	if final == io.EOF {
		return nil // clean close (or a repeat of one)
	}
	if installed && !clean {
		// The caller aborted with its own error: it already knows; the
		// reader is who must observe it. Never compare err against final
		// here — error values with non-comparable dynamic types (slices,
		// maps) are legal and interface comparison would panic.
		return nil
	}
	// ErrUnfilledGap on a nominally clean Close, or the sticky first error
	// on a repeated close.
	return final
}

// WaitForReader will block until the reader is closed. Returns the error set
// when the reader closed.
func (w *PipeWriterAt) WaitForReader() error {
	<-w.f.eor
	return w.f.readerror()
}

// debugging stuff
const watch = false

func trace(p ...interface{}) {
	if watch {
		fmt.Println(p...)
	}
}
