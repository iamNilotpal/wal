package pool

import (
	"bytes"
	"sync"
)

// BufferPool manages a pool of byte buffers.
type BufferPool struct {
	size int       // Size of each buffer.
	pool sync.Pool // Thread-safe pool of buffers.
}

// Creates a new buffer pool with a specified buffer size.
func NewBufferPool(size int) *BufferPool {
	return &BufferPool{
		size: size,
		pool: sync.Pool{
			New: func() any {
				return bytes.NewBuffer(make([]byte, 0, size))
			},
		},
	}
}

// Retrieves a buffer from the pool.
func (bp *BufferPool) Get() *bytes.Buffer {
	buf := bp.pool.Get().(*bytes.Buffer)
	buf.Reset() // Ensure the buffer is clean.
	return buf
}

// Returns a buffer to the pool.
func (bp *BufferPool) Put(buf *bytes.Buffer) {
	// Don't pool buffers that have grown too large.
	if buf.Cap() > bp.size*2 {
		return
	}

	buf.Reset()
	bp.pool.Put(buf)
}
