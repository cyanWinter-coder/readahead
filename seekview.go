// Copyright (c) 2015 Klaus Post, released under MIT License. See LICENSE file.

package readahead

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

// ErrViewClosed is returned by Read, Seek and WriteTo once a SeekView
// has been closed.
var ErrViewClosed = errors.New("readahead: seek view closed")

// SeekView is a cancellable, bidirectional read-ahead view on top of an
// io.ReadSeeker.
//
// Unlike the sequential reader returned by NewReader, a SeekView keeps a
// fixed-capacity window of blocks around the current logical offset,
// caching a small number of blocks behind the offset (backward window) and
// pre-fetching the remaining blocks ahead of it (forward window). A Seek
// re-centers the window on the seek target: blocks that fall outside the
// new window are dropped, overlapping blocks are kept (a single copy of
// each block is retained, no matter how the windows overlap), and
// in-flight pre-fetches that are no longer relevant are cancelled. Results
// arriving late from a cancelled pre-fetch are discarded and can never
// pollute the current offset.
//
// Read, Seek and WriteTo unblock immediately when the supplied context is
// cancelled or Close is called. Close stops using the underlying reader
// but never closes it; the caller retains ownership.
//
// A SeekView uses a single background goroutine for its entire lifetime,
// regardless of how many seeks are performed. All buffers are allocated
// up front and recycled internally.
//
// A SeekView is not safe for concurrent use by multiple goroutines,
// except that Close may be called at any time.
type SeekView struct {
	ctx context.Context
	rs  io.ReadSeeker

	size int64 // block size in bytes
	back int64 // blocks cached behind the current offset
	fwd  int64 // blocks pre-fetched ahead of/around the current offset

	fetchCh chan *viewBlock  // blocks to fetch, consumed by the dispatcher
	seekCh  chan viewSeekReq // underlying Seek(0, SeekEnd) requests
	done    chan struct{}    // closed by Close
	exited  chan struct{}    // closed when the dispatcher goroutine returns

	mu       sync.Mutex
	closed   bool
	pos      int64                // logical read offset
	blocks   map[int64]*viewBlock // cached/pending blocks by block index
	free     [][]byte             // recycled block buffers
	updated  chan struct{}        // closed and replaced on every window change
	eofPos   int64                // logical end of stream, if known
	eofKnown bool

	closeOnce sync.Once
}

// viewBlock is a single fixed-size block of the stream.
// data holds the filled portion of buf; err, if non-nil, must be
// delivered once data has been fully consumed.
type viewBlock struct {
	idx    int64
	buf    []byte // backing storage, len == block size
	data   []byte // filled portion of buf
	err    error  // terminal error for this block (io.EOF or read error)
	filled bool   // data and err are final
	ready  chan struct{}
}

// viewSeekReq asks the dispatcher (which owns the underlying reader) for
// the end offset of the stream.
type viewSeekReq struct {
	res chan viewSeekRes
}

type viewSeekRes struct {
	pos int64
	err error
}

// NewSeekView returns a cancellable, bidirectional read-ahead view over rs.
//
// buffers is the total number of blocks cached (fixed capacity) and size
// is the size of each block in bytes. Roughly a quarter of the blocks are
// used for the backward window and the rest for the forward window.
//
// The returned view reads from rs starting at its current offset.
// Read, Seek and WriteTo return promptly when ctx is cancelled or the
// view is closed. Close releases all internal resources but does NOT
// close rs.
func NewSeekView(ctx context.Context, rs io.ReadSeeker, buffers, size int) (*SeekView, error) {
	if rs == nil {
		return nil, fmt.Errorf("nil input reader supplied")
	}
	if size <= 0 {
		return nil, fmt.Errorf("buffer size too small")
	}
	if buffers <= 0 {
		return nil, fmt.Errorf("number of buffers too small")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	back := buffers / 4
	if back == 0 && buffers > 1 {
		back = 1
	}
	// Anchor the logical stream at the current offset of rs.
	pos, err := rs.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	v := &SeekView{
		ctx:     ctx,
		rs:      rs,
		pos:     pos,
		size:    int64(size),
		back:    int64(back),
		fwd:     int64(buffers - back),
		fetchCh: make(chan *viewBlock, buffers),
		seekCh:  make(chan viewSeekReq),
		done:    make(chan struct{}),
		exited:  make(chan struct{}),
		blocks:  make(map[int64]*viewBlock),
		updated: make(chan struct{}),
	}
	// Allocate the fixed buffer pool up front.
	x := make([]byte, buffers*size)
	for i := 0; i < buffers; i++ {
		v.free = append(v.free, x[i*size:(i+1)*size:(i+1)*size])
	}
	go v.run()
	v.mu.Lock()
	v.fillWindowLocked()
	v.mu.Unlock()
	return v, nil
}

// run is the single dispatcher goroutine. It is the only goroutine that
// touches the underlying reader, so seeks and reads on it are serialized.
func (v *SeekView) run() {
	defer close(v.exited)
	for {
		select {
		case <-v.done:
			return
		case <-v.ctx.Done():
			return
		default:
		}
		select {
		case <-v.done:
			return
		case <-v.ctx.Done():
			return
		case blk := <-v.fetchCh:
			v.fetch(blk)
		case req := <-v.seekCh:
			pos, err := v.rs.Seek(0, io.SeekEnd)
			// Buffered channel: never blocks, even if the requester
			// has been woken by Close or context cancellation.
			req.res <- viewSeekRes{pos: pos, err: err}
		}
	}
}

// fetch performs the I/O for a single block. If the block is no longer
// part of the window (evicted by a Seek) its buffer is recycled instead
// of being read into, and a result that arrives after the block was
// evicted is discarded.
func (v *SeekView) fetch(blk *viewBlock) {
	v.mu.Lock()
	if v.closed || v.blocks[blk.idx] != blk {
		v.free = append(v.free, blk.buf)
		v.broadcastLocked()
		v.mu.Unlock()
		return
	}
	v.mu.Unlock()

	data, err := v.readBlock(blk.idx*v.size, blk.buf)

	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed || v.blocks[blk.idx] != blk {
		// Stale result: the block was evicted while the read was in
		// flight. Recycle the buffer; never deliver to the window.
		v.free = append(v.free, blk.buf)
		v.fillWindowLocked()
		v.broadcastLocked()
		return
	}
	blk.data = data
	blk.err = err
	blk.filled = true
	close(blk.ready)
	if err == io.EOF {
		v.eofKnown = true
		v.eofPos = blk.idx*v.size + int64(len(data))
	}
	v.broadcastLocked()
}

// readBlock seeks to off and fills buf, looping over short reads.
// A panic in the underlying reader is recovered and returned as an error.
func (v *SeekView) readBlock(off int64, buf []byte) (data []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			data = buf[:0]
			err = fmt.Errorf("panic reading: %v", r)
		}
	}()
	if _, err = v.rs.Seek(off, io.SeekStart); err != nil {
		return buf[:0], err
	}
	n := 0
	for n < len(buf) {
		n2, err2 := v.rs.Read(buf[n:])
		n += n2
		if err2 != nil {
			return buf[:n], err2
		}
		if n2 == 0 {
			return buf[:n], io.ErrNoProgress
		}
	}
	return buf[:n], nil
}

// fillWindowLocked re-centers the block window on v.pos: blocks outside
// the window are evicted (ready buffers recycled at once, in-flight
// buffers recycled by fetch on completion) and missing blocks inside the
// window are requested. Must be called with v.mu held.
func (v *SeekView) fillWindowLocked() {
	if v.closed {
		return
	}
	anchor := v.pos / v.size
	lo := anchor - v.back
	if lo < 0 {
		lo = 0
	}
	hi := anchor + v.fwd
	if v.eofKnown {
		if last := v.eofPos/v.size + 1; hi > last {
			hi = last
		}
	}
	for idx, blk := range v.blocks {
		if idx < lo || idx >= hi {
			delete(v.blocks, idx)
			if blk.filled {
				v.free = append(v.free, blk.buf)
			}
			// Pending blocks keep their buffer until fetch completes
			// and recycles it, keeping the total capacity fixed.
		}
	}
	for idx := lo; idx < hi; idx++ {
		if _, ok := v.blocks[idx]; ok {
			continue
		}
		if len(v.free) == 0 {
			// All buffers are in flight; fetch completions will
			// re-trigger this and fill the rest.
			return
		}
		buf := v.free[len(v.free)-1]
		v.free = v.free[:len(v.free)-1]
		blk := &viewBlock{idx: idx, buf: buf, ready: make(chan struct{})}
		v.blocks[idx] = blk
		v.fetchCh <- blk
	}
}

// broadcastLocked wakes all goroutines waiting on window changes.
// Must be called with v.mu held.
func (v *SeekView) broadcastLocked() {
	close(v.updated)
	v.updated = make(chan struct{})
}

// stateErrLocked returns the terminal error of the view, if any.
func (v *SeekView) stateErrLocked() error {
	if v.closed {
		return ErrViewClosed
	}
	if err := v.ctx.Err(); err != nil {
		return err
	}
	return nil
}

// await blocks until the block containing the current offset has been
// filled, the end of the stream is reached, or the view is shut down.
func (v *SeekView) await() (*viewBlock, error) {
	for {
		v.mu.Lock()
		if err := v.stateErrLocked(); err != nil {
			v.mu.Unlock()
			return nil, err
		}
		idx := v.pos / v.size
		if v.eofKnown && v.pos >= v.eofPos {
			v.mu.Unlock()
			return nil, io.EOF
		}
		blk := v.blocks[idx]
		if blk != nil && blk.filled {
			v.mu.Unlock()
			return blk, nil
		}
		if blk == nil {
			// Make sure the block has been requested.
			v.fillWindowLocked()
			blk = v.blocks[idx]
		}
		var ready chan struct{}
		if blk != nil {
			ready = blk.ready
		}
		updated := v.updated
		v.mu.Unlock()

		select {
		case <-ready: // nil channel blocks forever, which is fine
		case <-updated:
		case <-v.done:
			return nil, ErrViewClosed
		case <-v.ctx.Done():
			return nil, v.ctx.Err()
		}
	}
}

// Read reads from the current logical offset. Data, short reads, errors
// and io.EOF are delivered in logical stream order. Read returns
// ErrViewClosed after Close and the context error after cancellation.
func (v *SeekView) Read(p []byte) (int, error) {
	if len(p) == 0 {
		v.mu.Lock()
		err := v.stateErrLocked()
		v.mu.Unlock()
		return 0, err
	}
	blk, err := v.await()
	if err != nil {
		return 0, err
	}
	v.mu.Lock()
	off := v.pos - blk.idx*v.size
	if off >= int64(len(blk.data)) {
		// The stream end was discovered after await returned, or the
		// offset sits in the short tail of a terminal block.
		err := blk.err
		v.mu.Unlock()
		if err == nil {
			err = io.EOF
		}
		return 0, err
	}
	n := copy(p, blk.data[off:])
	v.pos += int64(n)
	var rerr error
	if int(off)+n >= len(blk.data) {
		// Block exhausted: deliver its terminal error, if any.
		rerr = blk.err
	}
	v.fillWindowLocked()
	v.mu.Unlock()
	return n, rerr
}

// Seek sets the logical offset for the next Read. The window is
// re-centered on the new offset: pre-fetches that are no longer relevant
// are cancelled and blocks overlapping the new window are kept.
// Seeking past the end of the stream is allowed; subsequent reads return
// io.EOF. Seek returns ErrViewClosed after Close and the context error
// after cancellation.
func (v *SeekView) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		v.mu.Lock()
		abs = v.pos + offset
		v.mu.Unlock()
	case io.SeekEnd:
		end, err := v.streamEnd()
		if err != nil {
			return 0, err
		}
		abs = end + offset
	default:
		return 0, errors.New("readahead: invalid whence")
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.stateErrLocked(); err != nil {
		return 0, err
	}
	if abs < 0 {
		return 0, errors.New("readahead: seek to negative position")
	}
	v.pos = abs
	v.fillWindowLocked()
	v.broadcastLocked()
	return abs, nil
}

// streamEnd returns the end offset of the underlying stream, asking the
// dispatcher (which owns the underlying reader) when necessary.
func (v *SeekView) streamEnd() (int64, error) {
	v.mu.Lock()
	if v.eofKnown {
		end := v.eofPos
		v.mu.Unlock()
		return end, nil
	}
	if err := v.stateErrLocked(); err != nil {
		v.mu.Unlock()
		return 0, err
	}
	v.mu.Unlock()

	req := viewSeekReq{res: make(chan viewSeekRes, 1)}
	select {
	case v.seekCh <- req:
	case <-v.done:
		return 0, ErrViewClosed
	case <-v.ctx.Done():
		return 0, v.ctx.Err()
	}
	select {
	case r := <-req.res:
		return r.pos, r.err
	case <-v.done:
		return 0, ErrViewClosed
	case <-v.ctx.Done():
		return 0, v.ctx.Err()
	}
}

// WriteTo writes data to w until there's no more data to write or when an
// error occurs. The return value n is the number of bytes written.
// WriteTo unblocks immediately on Close or context cancellation.
func (v *SeekView) WriteTo(w io.Writer) (int64, error) {
	var total int64
	for {
		blk, err := v.await()
		if err != nil {
			if err == io.EOF {
				return total, nil
			}
			return total, err
		}
		v.mu.Lock()
		off := v.pos - blk.idx*v.size
		var data []byte
		if off < int64(len(blk.data)) {
			data = blk.data[off:]
		}
		v.mu.Unlock()
		if len(data) == 0 {
			if blk.err == io.EOF {
				return total, nil
			}
			if blk.err != nil {
				return total, blk.err
			}
			return total, io.EOF
		}
		n, werr := w.Write(data)
		total += int64(n)
		v.mu.Lock()
		v.pos += int64(n)
		v.fillWindowLocked()
		v.mu.Unlock()
		if werr != nil {
			return total, werr
		}
		if n < len(data) {
			return total, io.ErrShortWrite
		}
	}
}

// Close shuts the view down: Read, Seek and WriteTo calls blocked or
// issued afterwards return ErrViewClosed, the dispatcher goroutine exits
// and all buffers are released. The underlying reader is NOT closed.
// Close is safe to call multiple times.
func (v *SeekView) Close() error {
	v.closeOnce.Do(func() {
		v.mu.Lock()
		v.closed = true
		v.mu.Unlock()
		close(v.done)
	})
	<-v.exited
	v.mu.Lock()
	v.blocks = nil
	v.free = nil
	v.mu.Unlock()
	return nil
}
