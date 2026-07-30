package pipeat

// Fork test suite (discobean/pipeat).
//
// Two groups of tests:
//
//  1. Regression tests for the defects fixed in the fork (each maps to a
//     finding from the 2026-07 review).
//  2. Simulations of exactly how sfs-server uses the package:
//       - SFTP GET  = AsyncWriterPipe: a goroutine io.Copy's the backend
//         stream into the PipeWriterAt while the sftp layer issues
//         (possibly concurrent) fixed-size ReadAt calls.
//       - SFTP PUT  = Pipe (sync mode): the sftp layer issues concurrent,
//         possibly out-of-order WriteAt calls, then Close()s the writer and
//         relies on its error; a goroutine drains the PipeReaderAt into the
//         backend (sequential Read) and closes it with the backend's result.
//     Both directions also exercise the failure paths (backend errors,
//     client disconnects, panic-recovery double closes).

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	mrand "math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// helpers

func waitOrTimeout(t *testing.T, ch <-chan struct{}, d time.Duration, what string) bool {
	t.Helper()
	select {
	case <-ch:
		return true
	case <-time.After(d):
		t.Errorf("timed out after %v waiting for %s", d, what)
		return false
	}
}

// deterministic pseudo-random payload
func payload(n int) []byte {
	b := make([]byte, n)
	rng := mrand.New(mrand.NewSource(42))
	rng.Read(b)
	return b
}

// ---------------------------------------------------------------------------
// 1. Regression: exact-boundary reads must complete without writer close
//    (upstream off-by-one in waitForReadable)

func TestExactBoundaryReadCompletes(t *testing.T) {
	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)
	defer r.Close()
	defer w.Close()

	_, err = w.Write([]byte("abcd"))
	require.NoError(t, err)

	done := make(chan struct{})
	var n int
	var rerr error
	go func() {
		buf := make([]byte, 4)
		n, rerr = r.ReadAt(buf, 0)
		close(done)
	}()
	if waitOrTimeout(t, done, 2*time.Second, "exact-boundary ReadAt (writer still open)") {
		assert.Equal(t, 4, n)
		assert.NoError(t, rerr)
	}
}

func TestReadStillBlocksWhenDataInsufficient(t *testing.T) {
	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)
	defer w.Close()
	defer r.Close()

	_, err = w.Write([]byte("abc")) // 3 of the 4 requested bytes
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		r.ReadAt(make([]byte, 4), 0)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("ReadAt returned before the full range was written or an end closed")
	case <-time.After(150 * time.Millisecond):
		// still blocked: correct
	}
	_, err = w.Write([]byte("d"))
	require.NoError(t, err)
	waitOrTimeout(t, done, 2*time.Second, "ReadAt after the missing byte arrived")
}

// ---------------------------------------------------------------------------
// 2. Regression: unwritten holes must never be returned as data
//    (critical: silent zero-fill corruption)

func TestSparseWriteNeverReadAsZeros(t *testing.T) {
	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)
	defer r.Close()

	_, err = w.WriteAt([]byte("X"), 10) // bytes 0-9 never written
	require.NoError(t, err)
	closeErr := w.Close()
	assert.ErrorIs(t, closeErr, ErrUnfilledGap, "clean Close over a gap must fail closed")

	buf := make([]byte, 11)
	n, err := r.ReadAt(buf, 0)
	assert.Equal(t, 0, n, "no fabricated bytes may be returned")
	assert.ErrorIs(t, err, ErrUnfilledGap)
}

func TestGapErrorReachesSequentialReader(t *testing.T) {
	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)
	defer r.Close()

	_, err = w.WriteAt([]byte("tail"), 100)
	require.NoError(t, err)
	_, err = w.WriteAt([]byte("head"), 0)
	require.NoError(t, err)
	assert.ErrorIs(t, w.Close(), ErrUnfilledGap)

	// The contiguous head is still readable; the stream then errors instead
	// of fabricating the gap.
	head := make([]byte, 4)
	n, err := r.Read(head)
	assert.Equal(t, 4, n)
	assert.Equal(t, []byte("head"), head)
	if err == nil {
		_, err = r.Read(make([]byte, 8))
	}
	assert.ErrorIs(t, err, ErrUnfilledGap)
}

func TestNoGapCleanCloseIsEOF(t *testing.T) {
	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)
	defer r.Close()

	_, err = w.WriteAt([]byte("world"), 5) // ahead span...
	require.NoError(t, err)
	_, err = w.WriteAt([]byte("hello"), 0) // ...then filled: no gap
	require.NoError(t, err)
	assert.NoError(t, w.Close())

	buf := make([]byte, 10)
	n, err := r.ReadAt(buf, 0)
	assert.Equal(t, 10, n)
	assert.NoError(t, err)
	assert.Equal(t, []byte("helloworld"), buf)

	n, err = r.ReadAt(buf, 10)
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, io.EOF)
}

// ---------------------------------------------------------------------------
// 3. Regression: writer Close must surface the reader's terminal error
//    (sync mode: the sfs-server PUT contract — pkg/sftp treats the WriterAt
//    Close error as "was data lost?")

func TestWriterCloseReturnsUploaderError(t *testing.T) {
	r, w, err := Pipe()
	require.NoError(t, err)

	uploadErr := errors.New("s3: multipart upload failed")
	go func() {
		buf := make([]byte, 4)
		r.ReadAt(buf, 0) // consume, then fail like a broken uploader
		r.CloseWithError(uploadErr)
	}()

	_, err = w.Write([]byte("data"))
	require.NoError(t, err)
	assert.ErrorIs(t, w.Close(), uploadErr,
		"the SFTP layer must see the uploader's failure, not success")
}

func TestWriterCloseCleanReturnsNil(t *testing.T) {
	r, w, err := Pipe()
	require.NoError(t, err)

	go func() {
		io.Copy(io.Discard, r)
		r.Close()
	}()

	_, err = w.Write([]byte("data"))
	require.NoError(t, err)
	assert.NoError(t, w.Close())
}

func TestWriterCloseWithOwnErrorEchoesNil(t *testing.T) {
	// A writer aborting with its OWN error gets nil back (it already knows);
	// the reader is the one who must observe that error.
	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)
	defer r.Close()

	backendErr := errors.New("backend: GetObject failed")
	assert.NoError(t, w.CloseWithError(backendErr))

	n, err := r.ReadAt(make([]byte, 4), 0)
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, backendErr)
}

// ---------------------------------------------------------------------------
// 4. Regression: no write may succeed after close (either end, either API)

func TestWriteAfterWriterCloseFails(t *testing.T) {
	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)
	defer r.Close()

	require.NoError(t, w.Close())

	n, err := w.Write([]byte("late"))
	assert.Equal(t, 0, n)
	assert.Error(t, err, "Write after Close must fail")

	n, err = w.WriteAt([]byte("late"), 0)
	assert.Equal(t, 0, n)
	assert.Error(t, err, "WriteAt after Close must fail")

	assert.Equal(t, int64(0), w.f.endln, "no data may be published after close")
}

func TestWriteAfterReaderCloseReturnsReaderError(t *testing.T) {
	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)
	defer w.Close()

	cancelErr := errors.New("client disconnected")
	r.CloseWithError(cancelErr)

	n, err := w.Write([]byte("late"))
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, cancelErr)

	n, err = w.WriteAt([]byte("late"), 0)
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, cancelErr)
}

// ---------------------------------------------------------------------------
// 5. Regression: span accounting is a real interval union

func TestOverlappingSpansMergeAndUnblockReader(t *testing.T) {
	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)
	defer r.Close()
	defer w.Close()

	_, err = w.WriteAt(payload(15), 5) // [5,20) queued ahead
	require.NoError(t, err)
	_, err = w.WriteAt(payload(10), 0) // [0,10) OVERLAPS the queued span
	require.NoError(t, err)

	w.f.dataLock.RLock()
	endln, ahead := w.f.endln, len(w.f.ahead)
	w.f.dataLock.RUnlock()
	assert.Equal(t, int64(20), endln, "frontier must absorb the overlapping span")
	assert.Equal(t, 0, ahead, "the absorbed span must leave the queue")

	done := make(chan struct{})
	go func() {
		r.ReadAt(make([]byte, 20), 0)
		close(done)
	}()
	waitOrTimeout(t, done, 2*time.Second, "reader over the merged range (writer still open)")
}

func TestAheadQueueCoalescesAndStaysBounded(t *testing.T) {
	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)
	defer r.Close()
	defer w.Close()

	// Many overlapping rewrites of the same disjoint region.
	for i := 0; i < 100; i++ {
		_, err = w.WriteAt(payload(50), 1000)
		require.NoError(t, err)
	}
	// Plus a chain of overlapping spans.
	for i := 0; i < 100; i++ {
		_, err = w.WriteAt(payload(20), int64(2000+i*10))
		require.NoError(t, err)
	}
	w.f.dataLock.RLock()
	ahead := len(w.f.ahead)
	w.f.dataLock.RUnlock()
	assert.LessOrEqual(t, ahead, 2, "ahead queue must coalesce, not grow unbounded")
}

func TestRewriteBelowFrontierIsIgnored(t *testing.T) {
	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)
	defer r.Close()
	defer w.Close()

	_, err = w.WriteAt(payload(100), 0)
	require.NoError(t, err)
	_, err = w.WriteAt(payload(10), 20) // rewrite inside [0,100)
	require.NoError(t, err)

	w.f.dataLock.RLock()
	endln, ahead := w.f.endln, len(w.f.ahead)
	w.f.dataLock.RUnlock()
	assert.Equal(t, int64(100), endln)
	assert.Equal(t, 0, ahead)
}

func TestPublishSpanUnion(t *testing.T) {
	cases := []struct {
		name    string
		writes  []span // applied in order
		endln   int64
		aheadLn int
	}{
		{"sequential", []span{{0, 10}, {10, 20}}, 20, 0},
		{"exact overlap absorbed", []span{{5, 20}, {0, 5}}, 20, 0},
		{"overlap absorbed", []span{{5, 20}, {0, 10}}, 20, 0},
		{"chain absorbed", []span{{10, 20}, {20, 30}, {0, 10}}, 30, 0},
		{"disjoint stays queued", []span{{10, 20}}, 0, 1},
		{"queued spans coalesce", []span{{10, 20}, {15, 30}, {30, 40}}, 0, 1},
		{"below frontier dropped", []span{{0, 50}, {10, 20}}, 50, 0},
		{"straddling frontier extends", []span{{0, 10}, {5, 15}}, 15, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &pipeFile{eow: make(chan struct{}), eor: make(chan struct{})}
			f.rCond.L = f.dataLock.RLocker()
			f.wCond.L = f.dataLock.RLocker()
			for _, s := range tc.writes {
				f.publishSpan(s.start, s.end)
			}
			assert.Equal(t, tc.endln, f.endln, "endln")
			assert.Equal(t, tc.aheadLn, len(f.ahead), "ahead length: %v", f.ahead)
		})
	}
}

// ---------------------------------------------------------------------------
// 6. Regression: concurrent sequential readers are serialized (no race,
//    no duplicated bytes)

func TestConcurrentSequentialReaders(t *testing.T) {
	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)

	const total = 64 * 200
	go func() {
		src := payload(total)
		for off := 0; off < total; off += 64 {
			w.Write(src[off : off+64])
		}
		w.Close()
	}()

	var readTotal int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 8)
			for {
				n, err := r.Read(buf)
				mu.Lock()
				readTotal += int64(n)
				mu.Unlock()
				if err != nil {
					return
				}
			}
		}()
	}
	wg.Wait()
	r.Close()
	assert.Equal(t, int64(total), readTotal,
		"serialized sequential readers must consume each byte exactly once")
}

// ---------------------------------------------------------------------------
// 7. Regression: input validation

func TestNegativeAndOverflowOffsets(t *testing.T) {
	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)
	defer r.Close()
	defer w.Close()

	_, err = r.ReadAt(make([]byte, 4), -1)
	assert.ErrorIs(t, err, errNegativeOffset)
	_, err = w.WriteAt(make([]byte, 4), -1)
	assert.ErrorIs(t, err, errNegativeOffset)

	const nearMax = int64(^uint64(0) >> 1) // math.MaxInt64
	_, err = r.ReadAt(make([]byte, 4), nearMax)
	assert.ErrorIs(t, err, errOffsetOverflow)
	_, err = w.WriteAt(make([]byte, 4), nearMax)
	assert.ErrorIs(t, err, errOffsetOverflow)
}

func TestZeroLengthReadDoesNotBlock(t *testing.T) {
	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)
	defer r.Close()
	defer w.Close()

	done := make(chan struct{})
	var n int
	var rerr error
	go func() {
		n, rerr = r.ReadAt(nil, 0) // empty pipe, writer open
		close(done)
	}()
	if waitOrTimeout(t, done, 2*time.Second, "zero-length ReadAt on an open empty pipe") {
		assert.Equal(t, 0, n)
		assert.NoError(t, rerr)
	}
}

// ---------------------------------------------------------------------------
// 8. Regression: mixing Write and WriteAt keeps offsets consistent

func TestMixedWriteAndWriteAt(t *testing.T) {
	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)
	defer r.Close()

	_, err = w.Write([]byte("abc")) // sequential [0,3)
	require.NoError(t, err)
	_, err = w.WriteAt([]byte("d"), 3) // explicit [3,4)
	require.NoError(t, err)
	_, err = w.Write([]byte("e")) // sequential continues at 3... which is taken
	require.NoError(t, err)

	// Write()'s own offset is 3+1=4? No: Write tracks only its own bytes
	// (3 written), so "e" lands at offset 3, overwriting "d". That is the
	// documented semantic: Write is a sequential stream; WriteAt is random
	// access; interleaving them writes where each API's contract says.
	require.NoError(t, w.Close())

	buf := make([]byte, 4)
	n, err := r.ReadAt(buf, 0)
	assert.Equal(t, 4, n)
	assert.NoError(t, err)
	assert.Equal(t, []byte("abce"), buf,
		"Write()'s stream offset is independent of WriteAt; no file-cursor corruption")
}

// ---------------------------------------------------------------------------
// 9. Regression: close idempotency + panic-recovery double closes

func TestPanicRecoveryStyleDoubleClose(t *testing.T) {
	// Mirrors sfs-server's goroutines: CloseWithError from a recover() path
	// can race or follow a normal Close. Nothing may panic; the first error
	// must stick.
	r, w, err := Pipe()
	require.NoError(t, err)

	uploadErr := errors.New("s3: PutObject failed")
	panicErr := errors.New("recovered panic")
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); r.CloseWithError(uploadErr) }()
	go func() { defer wg.Done(); r.Close() }()
	go func() { defer wg.Done(); r.CloseWithError(panicErr) }()
	wg.Wait()

	// First close wins and is sticky: the terminal error must be exactly one
	// of the three racers, and the writer's Close must agree with it — an
	// error iff the winning close was not the clean one. (In sfs-server the
	// clean and error closes are sequential, never racing; this test only
	// pins down that a race can't panic or produce an out-of-thin-air state.)
	terminal := w.f.readerror()
	require.Error(t, terminal, "a terminal reader state must be set")
	closeErr := w.Close()
	switch {
	case errors.Is(terminal, uploadErr):
		assert.ErrorIs(t, closeErr, uploadErr)
	case errors.Is(terminal, panicErr):
		assert.ErrorIs(t, closeErr, panicErr)
	case errors.Is(terminal, io.EOF):
		assert.NoError(t, closeErr)
	default:
		t.Errorf("terminal reader error %v is none of the racing closes", terminal)
	}

	// And double-closing the writer afterwards must stay safe.
	assert.NotPanics(t, func() { w.Close(); w.CloseWithError(errors.New("again")) })
}

func TestReaderCloseIdempotent(t *testing.T) {
	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)
	defer w.Close()

	assert.NoError(t, r.Close())
	assert.NoError(t, r.Close(), "second Close must repeat the first result, not fs.ErrClosed")
	assert.NoError(t, r.CloseWithError(errors.New("late")), "late CloseWithError must not disturb the fd result")
	assert.ErrorIs(t, r.f.readerror(), io.EOF, "first close's error must be sticky")
}

// ---------------------------------------------------------------------------
// 10. Close unblocks blocked peers (client-disconnect paths)

func TestReaderCloseUnblocksBlockedReadAt(t *testing.T) {
	// sfs-server GET: client disconnects; pkg/sftp closes the ReaderAt while
	// another ReadAt is still blocked waiting for backend data.
	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)
	defer w.Close()

	done := make(chan struct{})
	var rerr error
	go func() {
		_, rerr = r.ReadAt(make([]byte, 64), 0) // blocks: nothing written
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	cancelErr := errors.New("session torn down")
	r.CloseWithError(cancelErr)
	if waitOrTimeout(t, done, 2*time.Second, "blocked ReadAt after reader close") {
		assert.ErrorIs(t, rerr, cancelErr)
	}
}

func TestWriterCloseUnblocksBlockedReadAt(t *testing.T) {
	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)
	defer r.Close()

	done := make(chan struct{})
	var rerr error
	go func() {
		_, rerr = r.ReadAt(make([]byte, 64), 0)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	backendErr := errors.New("backend read failed")
	w.CloseWithError(backendErr)
	if waitOrTimeout(t, done, 2*time.Second, "blocked ReadAt after writer close") {
		assert.ErrorIs(t, rerr, backendErr)
	}
}

func TestReaderCloseUnblocksBlockedWriter(t *testing.T) {
	// sync mode: writer is throttled waiting for the reader to request more;
	// the reader (uploader) dies instead.
	r, w, err := Pipe()
	require.NoError(t, err)

	_, err = w.Write(payload(64)) // gets ahead of the (nonexistent) read demand
	require.NoError(t, err)

	done := make(chan struct{})
	var werr error
	go func() {
		_, werr = w.Write(payload(64)) // blocks in waitForWritable
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	uploadErr := errors.New("uploader crashed")
	r.CloseWithError(uploadErr)
	if waitOrTimeout(t, done, 2*time.Second, "blocked Write after reader close") {
		assert.ErrorIs(t, werr, uploadErr)
	}
}

// ---------------------------------------------------------------------------
// 11. sfs-server simulation: SFTP GET (AsyncWriterPipe)

func TestSimGetDownloadStream(t *testing.T) {
	// Backend goroutine io.Copy's into the writer; the sftp layer issues
	// pipelined concurrent fixed-size ReadAt calls for successive offsets.
	const size = 2 << 20 // 2 MiB
	const chunk = 32 * 1024
	src := payload(size)

	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)

	writerDone := make(chan error, 1)
	go func() {
		// backend stream in odd-sized pieces, like a real HTTP body
		_, copyErr := io.Copy(w, io.LimitReader(bytes.NewReader(src), size))
		if copyErr != nil {
			w.CloseWithError(copyErr)
			writerDone <- copyErr
			return
		}
		writerDone <- w.Close()
	}()

	out := make([]byte, size)
	var wg sync.WaitGroup
	errCh := make(chan error, 2*(size/chunk)+2) // each goroutine may report twice
	sem := make(chan struct{}, 8)               // sftp-style bounded pipelining
	for off := 0; off < size; off += chunk {
		wg.Add(1)
		go func(off int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			end := off + chunk
			if end > size {
				end = size
			}
			n, err := r.ReadAt(out[off:end], int64(off))
			if err != nil && !errors.Is(err, io.EOF) {
				errCh <- fmt.Errorf("ReadAt(%d): %w", off, err)
			}
			if n != end-off {
				errCh <- fmt.Errorf("ReadAt(%d): short read %d", off, n)
			}
		}(off)
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		t.Error(e)
	}
	// Join the backend writer so the writer is definitely closed, then test
	// the past-EOF read while the reader is still open — this exercises the
	// writer-frontier `off >= endln` branch, not the reader-closed path.
	require.NoError(t, <-writerDone)
	n, err := r.ReadAt(make([]byte, 8), int64(size))
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, io.EOF, "past-EOF with a cleanly closed writer must be io.EOF")

	require.NoError(t, r.Close())
	assert.Equal(t, sha256.Sum256(src), sha256.Sum256(out), "downloaded bytes must match the backend stream")
}

func TestSimGetBackendFailurePropagates(t *testing.T) {
	// Backend dies mid-stream: the client must see the error, never a
	// truncated-but-successful file.
	const half = 64 * 1024
	src := payload(half)

	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)
	defer r.Close()

	backendErr := errors.New("backend: connection reset")
	go func() {
		w.Write(src)
		w.CloseWithError(backendErr)
	}()

	// first half arrives fine
	buf := make([]byte, half)
	n, err := r.ReadAt(buf, 0)
	require.Equal(t, half, n)
	require.NoError(t, err)

	// the read past the failure point reports the backend error
	n, err = r.ReadAt(buf, int64(half))
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, backendErr)
}

// ---------------------------------------------------------------------------
// 12. sfs-server simulation: SFTP PUT (Pipe, sync mode)

func TestSimPutOutOfOrderConcurrentWrites(t *testing.T) {
	// The sftp layer writes 32k packets concurrently and out of order;
	// the uploader drains sequentially with Read. Data must round-trip
	// bit-exact and the writer's Close must succeed.
	const size = 2 << 20
	const chunk = 32 * 1024
	src := payload(size)

	r, w, err := Pipe()
	require.NoError(t, err)

	// uploader (S3 stand-in): sequential Read into a hash
	uploaded := &bytes.Buffer{}
	uploadDone := make(chan error, 1)
	go func() {
		_, err := io.Copy(uploaded, r)
		if err != nil {
			r.CloseWithError(err)
			uploadDone <- err
			return
		}
		uploadDone <- r.Close()
	}()

	// sftp layer: shuffled chunk order, bounded concurrency
	offs := make([]int, 0, size/chunk)
	for off := 0; off < size; off += chunk {
		offs = append(offs, off)
	}
	rng := mrand.New(mrand.NewSource(7))
	rng.Shuffle(len(offs), func(i, j int) { offs[i], offs[j] = offs[j], offs[i] })

	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	errCh := make(chan error, len(offs))
	for _, off := range offs {
		wg.Add(1)
		go func(off int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			end := off + chunk
			if end > size {
				end = size
			}
			if _, err := w.WriteAt(src[off:end], int64(off)); err != nil {
				errCh <- fmt.Errorf("WriteAt(%d): %w", off, err)
			}
		}(off)
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		t.Error(e)
	}

	assert.NoError(t, w.Close(), "upload completed: Close must report success")
	require.NoError(t, <-uploadDone)
	assert.Equal(t, sha256.Sum256(src), sha256.Sum256(uploaded.Bytes()),
		"uploaded bytes must match what the client wrote")

	w.f.dataLock.RLock()
	assert.Empty(t, w.f.ahead, "all spans must have merged")
	w.f.dataLock.RUnlock()
}

func TestSimPutUploaderFailurePropagates(t *testing.T) {
	// S3 fails mid-upload: pending and subsequent writes must error, and
	// the writer's Close must report the failure to the sftp layer.
	r, w, err := Pipe()
	require.NoError(t, err)

	uploadErr := errors.New("s3: 503 slow down")
	go func() {
		buf := make([]byte, 1024)
		r.Read(buf) // accept the first chunk
		r.CloseWithError(uploadErr)
	}()

	deadline := time.After(5 * time.Second)
	var lastErr error
	for lastErr == nil {
		select {
		case <-deadline:
			t.Fatal("writes kept succeeding after the uploader failed")
		default:
		}
		_, lastErr = w.Write(payload(1024))
	}
	assert.ErrorIs(t, lastErr, uploadErr)
	assert.ErrorIs(t, w.Close(), uploadErr,
		"Close must return the uploader's error to the SFTP layer")
}

func TestSimPutClientAbandonsMidUpload(t *testing.T) {
	// Client disconnects without finishing; sfs-server's recovery closes
	// the reader from the goroutine. Everything must unwind, no goroutine
	// stuck in WaitForReader.
	r, w, err := Pipe()
	require.NoError(t, err)

	_, err = w.WriteAt(payload(1024), 0)
	require.NoError(t, err)

	abortErr := errors.New("session aborted")
	closed := make(chan struct{})
	go func() {
		// uploader notices the death and closes with an error
		r.CloseWithError(abortErr)
		close(closed)
	}()
	waitOrTimeout(t, closed, 2*time.Second, "reader close")

	done := make(chan error, 1)
	go func() {
		done <- w.Close() // must not hang in WaitForReader
	}()
	select {
	case closeErr := <-done:
		// The authoritative close result must carry the abort reason.
		assert.ErrorIs(t, closeErr, abortErr)
	case <-time.After(2 * time.Second):
		t.Error("writer close hung after reader death")
	}
}

// ---------------------------------------------------------------------------
// 12b. Close error classification regressions

// sliceErr is a legal error implementation with a NON-comparable dynamic type;
// interface comparison against it panics. Writer close must never compare
// arbitrary error values.
type sliceErr []byte

func (sliceErr) Error() string { return "slice-backed failure" }

func TestNonComparableErrorDoesNotPanicClose(t *testing.T) {
	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)
	defer r.Close()

	assert.NotPanics(t, func() {
		assert.NoError(t, w.CloseWithError(sliceErr{1}), "own-error close echoes nil")
	})
	// and the reader observes it
	n, rerr := r.ReadAt(make([]byte, 4), 0)
	assert.Equal(t, 0, n)
	var se sliceErr
	assert.ErrorAs(t, rerr, &se)
}

func TestWrappedEOFReaderErrorIsNotSwallowed(t *testing.T) {
	// An uploader failure that happens to WRAP io.EOF (e.g. "upload failed:
	// EOF" bubbled out of a body read) is a failure, not a clean close.
	r, w, err := Pipe()
	require.NoError(t, err)

	wrapped := fmt.Errorf("object upload failed: %w", io.EOF)
	go func() {
		r.ReadAt(make([]byte, 4), 0)
		r.CloseWithError(wrapped)
	}()

	_, err = w.Write([]byte("data"))
	require.NoError(t, err)
	closeErr := w.Close()
	assert.Error(t, closeErr, "wrapped-EOF reader failure must surface from writer Close")
	assert.ErrorIs(t, closeErr, wrapped)
}

func TestCloseWithLiteralEOFStillDetectsGap(t *testing.T) {
	// CloseWithError(io.EOF) is a clean close by contract, so it must run
	// gap detection exactly like Close() — not slip past it.
	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)
	defer r.Close()

	_, err = w.WriteAt([]byte("tail"), 100) // gap below
	require.NoError(t, err)
	assert.ErrorIs(t, w.CloseWithError(io.EOF), ErrUnfilledGap)

	n, err := r.ReadAt(make([]byte, 4), 0)
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, ErrUnfilledGap)
}

// ---------------------------------------------------------------------------
// 12c. Coverage gaps from the round-2 review

func TestSimGetZeroByteFile(t *testing.T) {
	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)

	require.NoError(t, w.Close()) // empty backend object, no gap

	n, err := r.ReadAt(make([]byte, 8), 0)
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, io.EOF)
	require.NoError(t, r.Close())
}

func TestSimPutZeroByteFile(t *testing.T) {
	r, w, err := Pipe()
	require.NoError(t, err)

	uploadDone := make(chan error, 1)
	go func() {
		n, copyErr := io.Copy(io.Discard, r)
		if copyErr != nil {
			r.CloseWithError(copyErr)
			uploadDone <- copyErr
			return
		}
		if n != 0 {
			uploadDone <- fmt.Errorf("expected empty stream, got %d bytes", n)
			r.Close()
			return
		}
		uploadDone <- r.Close()
	}()

	assert.NoError(t, w.Close(), "zero-byte upload must close cleanly")
	assert.NoError(t, <-uploadDone)
}

func TestHugeSparseOffsetSafe(t *testing.T) {
	// A write parked at a huge offset must not overflow, panic, or fabricate
	// gigabytes of zeros — it is an unfilled gap like any other. (The temp
	// file stays sparse on disk, so this is cheap.)
	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)
	defer r.Close()

	const far = int64(1) << 33 // 8 GiB
	_, err = w.WriteAt([]byte("tail"), far)
	require.NoError(t, err)
	assert.ErrorIs(t, w.Close(), ErrUnfilledGap)

	n, err := r.ReadAt(make([]byte, 4), 0)
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, ErrUnfilledGap)
}

func TestSimGetShortFinalChunk(t *testing.T) {
	// File size not a multiple of the request size: the final ReadAt must
	// return the short tail together with io.EOF, like a real file.
	const size = 100_000 // 3×32768 + 1696
	const chunk = 32 * 1024
	src := payload(size)

	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)

	go func() {
		w.Write(src)
		w.Close()
	}()

	out := make([]byte, 0, size)
	buf := make([]byte, chunk)
	off := int64(0)
	for {
		n, err := r.ReadAt(buf, off)
		out = append(out, buf[:n]...)
		off += int64(n)
		if err != nil {
			assert.ErrorIs(t, err, io.EOF)
			assert.Equal(t, size%chunk, n, "final chunk must be the short tail")
			break
		}
		assert.Equal(t, chunk, n)
	}
	require.NoError(t, r.Close())
	assert.Equal(t, sha256.Sum256(src), sha256.Sum256(out))
}

func TestReadPartialDataWithTerminalError(t *testing.T) {
	// Writer delivers some bytes then dies: a read spanning the failure
	// point must return the real bytes AND the terminal error together
	// (short read with non-nil error — the ReaderAt contract).
	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)
	defer r.Close()

	backendErr := errors.New("backend: stream cut")
	_, err = w.Write(payload(10))
	require.NoError(t, err)
	w.CloseWithError(backendErr)

	buf := make([]byte, 20)
	n, err := r.ReadAt(buf, 0)
	assert.Equal(t, 10, n, "the bytes that did arrive must be delivered")
	assert.ErrorIs(t, err, backendErr)
}

// ---------------------------------------------------------------------------
// 13. stats survive the rework (sfs-server logs both)

func TestByteCountersMatch(t *testing.T) {
	r, w, err := AsyncWriterPipe()
	require.NoError(t, err)

	const size = 100 * 1024
	src := payload(size)
	go func() {
		w.Write(src)
		w.Close()
	}()

	n, err := io.Copy(io.Discard, r)
	require.NoError(t, err)
	require.NoError(t, r.Close())

	assert.Equal(t, int64(size), n)
	assert.Equal(t, int64(size), w.GetWrittenBytes())
	assert.Equal(t, int64(size), r.GetReadedBytes())
}
