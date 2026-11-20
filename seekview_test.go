package readahead_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/readahead"
)

// seekable wraps a io.ReadSeeker with customizable Read behavior.
type seekableWrapper struct {
	rs     io.ReadSeeker
	readFn func(io.ReadSeeker, []byte) (int, error)
}

func (s *seekableWrapper) Read(p []byte) (int, error) { return s.readFn(s.rs, p) }
func (s *seekableWrapper) Seek(off int64, whence int) (int64, error) {
	return s.rs.Seek(off, whence)
}

func halfRead(rs io.ReadSeeker, p []byte) (int, error) {
	return rs.Read(p[:(len(p)+1)/2])
}

func oneByteRead(rs io.ReadSeeker, p []byte) (int, error) {
	return rs.Read(p[:1])
}

func makeData(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i * 31)
	}
	return b
}

func TestSeekViewSequential(t *testing.T) {
	data := makeData(100003) // not a multiple of the block size
	for _, fn := range []struct {
		name string
		fn   func(io.ReadSeeker, []byte) (int, error)
	}{
		{"full", func(rs io.ReadSeeker, p []byte) (int, error) { return rs.Read(p) }},
		{"half", halfRead},
		{"byte", oneByteRead},
	} {
		t.Run(fn.name, func(t *testing.T) {
			src := &seekableWrapper{rs: bytes.NewReader(data), readFn: fn.fn}
			v, err := readahead.NewSeekView(context.Background(), src, 4, 1000)
			if err != nil {
				t.Fatal(err)
			}
			defer v.Close()
			got, err := io.ReadAll(v)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, data) {
				t.Fatalf("got %d bytes, want %d; equal=%v", len(got), len(data), bytes.Equal(got, data))
			}
			// EOF must be sticky while positioned at the end.
			if n, err := v.Read(make([]byte, 1)); n != 0 || err != io.EOF {
				t.Fatalf("want (0, EOF), got (%d, %v)", n, err)
			}
		})
	}
}

func TestSeekViewRandomSeeks(t *testing.T) {
	data := makeData(50000)
	rng := rand.New(rand.NewSource(42))
	for _, cfg := range []struct{ buffers, size int }{
		{2, 100},
		{4, 1000},
		{8, 997},
	} {
		src := bytes.NewReader(data)
		v, err := readahead.NewSeekView(context.Background(), src, cfg.buffers, cfg.size)
		if err != nil {
			t.Fatal(err)
		}
		control := bytes.NewReader(data)
		buf := make([]byte, 300)
		cbuf := make([]byte, 300)
		for i := 0; i < 200; i++ {
			var pos int64
			var err error
			switch rng.Intn(4) {
			case 0:
				pos, err = v.Seek(rng.Int63n(int64(len(data))), io.SeekStart)
			case 1:
				pos, err = v.Seek(rng.Int63n(2000)-1000, io.SeekCurrent)
			case 2:
				pos, err = v.Seek(-rng.Int63n(int64(len(data))), io.SeekEnd)
			default:
				pos, err = v.Seek(0, io.SeekCurrent)
			}
			if err != nil {
				// SeekCurrent may legitimately land before the
				// start of the stream; keep the old position.
				continue
			}
			cpos, _ := control.Seek(pos, io.SeekStart)
			if cpos != pos {
				t.Fatalf("control seek mismatch: %d != %d", cpos, pos)
			}
			n, err := io.ReadFull(v, buf)
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				t.Fatal(err)
			}
			cn, _ := io.ReadFull(control, cbuf)
			if n != cn || !bytes.Equal(buf[:n], cbuf[:cn]) {
				t.Fatalf("iter %d: got %d bytes, want %d", i, n, cn)
			}
		}
		if err := v.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// gateSeeker blocks Read calls until released.
type gateSeeker struct {
	rs      io.ReadSeeker
	mu      sync.Mutex
	gate    chan struct{} // when non-nil, Read waits for it to be closed
	entered chan struct{} // optional: signalled (non-blocking) on each Read
}

func (g *gateSeeker) Read(p []byte) (int, error) {
	g.mu.Lock()
	ch := g.gate
	entered := g.entered
	g.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if ch != nil {
		<-ch
	}
	return g.rs.Read(p)
}

func (g *gateSeeker) Seek(off int64, whence int) (int64, error) {
	return g.rs.Seek(off, whence)
}

func (g *gateSeeker) block() {
	g.mu.Lock()
	g.gate = make(chan struct{})
	g.mu.Unlock()
}

func (g *gateSeeker) release() {
	g.mu.Lock()
	if g.gate != nil {
		close(g.gate)
		g.gate = nil
	}
	g.mu.Unlock()
}

// Seeking away must cancel irrelevant in-flight pre-fetches, and late
// results must not pollute the new offset.
func TestSeekViewSeekCancelsStalePrefetch(t *testing.T) {
	data := makeData(100000)
	g := &gateSeeker{rs: bytes.NewReader(data), entered: make(chan struct{}, 1)}
	// Block the underlying reader up front so the initial pre-fetches
	// are stuck in flight.
	g.block()
	v, err := readahead.NewSeekView(context.Background(), g, 4, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	// Wait until the dispatcher is stuck inside the gated Read.
	<-g.entered

	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		v.Read(make([]byte, 10))
	}()

	// Seek far away; the in-flight and queued pre-fetches are now stale.
	pos, err := v.Seek(50000, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 50000 {
		t.Fatalf("want pos 50000, got %d", pos)
	}
	// Release the gate: stale results arrive late and must be discarded.
	g.release()
	<-readDone // the pre-seek Read may consume some bytes at the new offset

	// Re-seek and verify the data at the new offset is exactly right.
	if _, err := v.Seek(50000, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 2000)
	if _, err := io.ReadFull(v, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data[50000:52000]) {
		t.Fatal("stale pre-fetch polluted the current offset")
	}
}

func TestSeekViewContextCancelWakesRead(t *testing.T) {
	data := makeData(100000)
	g := &gateSeeker{rs: bytes.NewReader(data), entered: make(chan struct{}, 1)}
	g.block()
	ctx, cancel := context.WithCancel(context.Background())
	v, err := readahead.NewSeekView(ctx, g, 4, 1000)
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() {
		_, err := v.Read(make([]byte, 10))
		errCh <- err
	}()
	// Wait until the dispatcher is stuck inside the gated Read.
	<-g.entered
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Read did not wake on context cancel")
	}
	// Seek and WriteTo must also fail fast after cancellation.
	if _, err := v.Seek(0, io.SeekStart); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled from Seek, got %v", err)
	}
	if _, err := v.WriteTo(io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled from WriteTo, got %v", err)
	}
	g.release()
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSeekViewCloseWakesBlockedCalls(t *testing.T) {
	data := makeData(100000)
	g := &gateSeeker{rs: bytes.NewReader(data), entered: make(chan struct{}, 1)}
	g.block()
	v, err := readahead.NewSeekView(context.Background(), g, 4, 1000)
	if err != nil {
		t.Fatal(err)
	}
	// Wait until the dispatcher is stuck inside the gated Read.
	<-g.entered

	readErr := make(chan error, 1)
	seekErr := make(chan error, 1)
	writeErr := make(chan error, 1)
	go func() {
		_, err := v.Read(make([]byte, 10))
		readErr <- err
	}()
	go func() {
		// SeekEnd needs the dispatcher, which is blocked in Read.
		_, err := v.Seek(0, io.SeekEnd)
		seekErr <- err
	}()
	go func() {
		_, err := v.WriteTo(io.Discard)
		writeErr <- err
	}()
	time.Sleep(50 * time.Millisecond)

	closeDone := make(chan struct{})
	go func() {
		v.Close()
		close(closeDone)
	}()
	for name, ch := range map[string]chan error{"Read": readErr, "Seek": seekErr, "WriteTo": writeErr} {
		select {
		case err := <-ch:
			if !errors.Is(err, readahead.ErrViewClosed) {
				t.Fatalf("%s: want ErrViewClosed, got %v", name, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s did not wake on Close", name)
		}
	}
	// The dispatcher is still stuck in the gated Read; release it so
	// Close can finish.
	g.release()
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the underlying reader unblocked")
	}
	// Calls after Close must fail, not panic.
	if _, err := v.Read(make([]byte, 1)); !errors.Is(err, readahead.ErrViewClosed) {
		t.Fatalf("want ErrViewClosed after Close, got %v", err)
	}
	if _, err := v.Seek(1, io.SeekStart); !errors.Is(err, readahead.ErrViewClosed) {
		t.Fatalf("want ErrViewClosed after Close, got %v", err)
	}
	if err := v.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

type seekCloser struct {
	io.ReadSeeker
	closed bool
}

func (s *seekCloser) Close() error {
	s.closed = true
	return nil
}

// Close must stop using the underlying reader but never close it.
func TestSeekViewCloseDoesNotCloseUnderlying(t *testing.T) {
	data := makeData(10000)
	sc := &seekCloser{ReadSeeker: bytes.NewReader(data)}
	v, err := readahead.NewSeekView(context.Background(), sc, 4, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.Read(make([]byte, 10)); err != nil {
		t.Fatal(err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if sc.closed {
		t.Fatal("underlying reader was closed by the view")
	}
}

// Random seeks must not grow the goroutine count, and Close must release
// the dispatcher goroutine.
func TestSeekViewGoroutineStability(t *testing.T) {
	data := makeData(200000)
	base := runtime.NumGoroutine()
	v, err := readahead.NewSeekView(context.Background(), bytes.NewReader(data), 4, 512)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	afterCreate := runtime.NumGoroutine()
	rng := rand.New(rand.NewSource(7))
	buf := make([]byte, 100)
	for i := 0; i < 500; i++ {
		if _, err := v.Seek(rng.Int63n(int64(len(data))), io.SeekStart); err != nil {
			t.Fatal(err)
		}
		if _, err := v.Read(buf); err != nil && err != io.EOF {
			t.Fatal(err)
		}
	}
	if got := runtime.NumGoroutine(); got > afterCreate {
		t.Fatalf("goroutines grew during random seeks: %d -> %d", afterCreate, got)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > base && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > base {
		t.Fatalf("goroutine leak after Close: base %d, now %d", base, got)
	}
}

// Errors and EOF must be delivered in logical stream order.
func TestSeekViewOrderedErrorDelivery(t *testing.T) {
	data := makeData(10000)
	theErr := errors.New("boom")
	var mu sync.Mutex
	var reads int
	src := &seekableWrapper{
		rs: bytes.NewReader(data),
		readFn: func(rs io.ReadSeeker, p []byte) (int, error) {
			mu.Lock()
			reads++
			fail := reads > 3
			mu.Unlock()
			if fail {
				return 0, theErr
			}
			return rs.Read(p)
		},
	}
	v, err := readahead.NewSeekView(context.Background(), src, 2, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	var got bytes.Buffer
	for {
		b := make([]byte, 700)
		n, err := v.Read(b)
		got.Write(b[:n])
		if err != nil {
			if err != theErr {
				t.Fatalf("want %v, got %v", theErr, err)
			}
			break
		}
	}
	// Three 1000-byte blocks were fetched before the error.
	if got.Len() != 3000 {
		t.Fatalf("want 3000 bytes before the error, got %d", got.Len())
	}
	if !bytes.Equal(got.Bytes(), data[:3000]) {
		t.Fatal("data before the error was reordered or corrupted")
	}
}

func TestSeekViewPanicRecovery(t *testing.T) {
	src := &seekableWrapper{
		rs: bytes.NewReader(makeData(100)),
		readFn: func(rs io.ReadSeeker, p []byte) (int, error) {
			panic("underlying panic")
		},
	}
	v, err := readahead.NewSeekView(context.Background(), src, 2, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	_, err = v.Read(make([]byte, 10))
	if err == nil {
		t.Fatal("want error from panicking reader, got nil")
	}
}

func TestSeekViewSeekEndAndWriteTo(t *testing.T) {
	data := makeData(5432)
	v, err := readahead.NewSeekView(context.Background(), bytes.NewReader(data), 4, 512)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	pos, err := v.Seek(-1000, io.SeekEnd)
	if err != nil {
		t.Fatal(err)
	}
	if pos != int64(len(data)-1000) {
		t.Fatalf("want %d, got %d", len(data)-1000, pos)
	}
	var dst bytes.Buffer
	n, err := v.WriteTo(&dst)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1000 || !bytes.Equal(dst.Bytes(), data[len(data)-1000:]) {
		t.Fatalf("WriteTo: got %d bytes, equal=%v", n, bytes.Equal(dst.Bytes(), data[len(data)-1000:]))
	}
}

func TestSeekViewBackwardWindowReuse(t *testing.T) {
	data := makeData(10000)
	var mu sync.Mutex
	var reads int
	src := &seekableWrapper{
		rs: bytes.NewReader(data),
		readFn: func(rs io.ReadSeeker, p []byte) (int, error) {
			mu.Lock()
			reads++
			mu.Unlock()
			return rs.Read(p)
		},
	}
	v, err := readahead.NewSeekView(context.Background(), src, 8, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	// Read forward 5 blocks, then seek back 2 blocks: the backward
	// window should serve it without new I/O.
	if _, err := io.CopyN(io.Discard, v, 5000); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	before := reads
	mu.Unlock()
	if _, err := v.Seek(-2000, io.SeekCurrent); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 1000)
	if _, err := io.ReadFull(v, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data[3000:4000]) {
		t.Fatal("backward read returned wrong data")
	}
	mu.Lock()
	after := reads
	mu.Unlock()
	if after != before {
		t.Fatalf("backward window miss: reads went %d -> %d", before, after)
	}
}

func TestSeekViewErrors(t *testing.T) {
	if _, err := readahead.NewSeekView(context.Background(), nil, 4, 100); err == nil {
		t.Fatal("want error for nil reader")
	}
	src := bytes.NewReader(makeData(10))
	if _, err := readahead.NewSeekView(context.Background(), src, 0, 100); err == nil {
		t.Fatal("want error for 0 buffers")
	}
	if _, err := readahead.NewSeekView(context.Background(), src, 4, 0); err == nil {
		t.Fatal("want error for 0 size")
	}
	v, err := readahead.NewSeekView(context.Background(), src, 4, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	if _, err := v.Seek(-1, io.SeekStart); err == nil {
		t.Fatal("want error for negative seek")
	}
	if _, err := v.Seek(0, 42); err == nil {
		t.Fatal("want error for invalid whence")
	}
}
