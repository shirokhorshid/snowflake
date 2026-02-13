package snowflake_proxy

import (
	"context"
	"io"
	"sync"

	"golang.org/x/time/rate"
)

const (
	// DefaultBurstSize is the maximum number of bytes that can be consumed
	// in a single burst. This should be large enough to avoid excessive
	// blocking on each io.Copy iteration, but small enough that the limiter
	// remains responsive to rate changes.
	DefaultBurstSize = 64 * 1024 // 64 KB
)

// BandwidthManager enforces a global bandwidth limit shared fairly across
// all connected clients. When clients join or leave, each client's individual
// rate limiter is rebalanced to globalLimit / numClients.
type BandwidthManager struct {
	mu                sync.Mutex
	globalBytesPerSec int64
	clients           []*ClientLimiter
}

// NewBandwidthManager creates a BandwidthManager with the given global
// bandwidth limit in bytes per second. A value <= 0 means unlimited
// (no limiting will be applied).
func NewBandwidthManager(globalBytesPerSec int64) *BandwidthManager {
	return &BandwidthManager{
		globalBytesPerSec: globalBytesPerSec,
	}
}

// AddClient registers a new client and returns its ClientLimiter.
// All existing client limiters are rebalanced to accommodate the new client.
func (bm *BandwidthManager) AddClient() *ClientLimiter {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	cl := &ClientLimiter{}
	bm.clients = append(bm.clients, cl)
	bm.rebalanceLocked()
	return cl
}

// RemoveClient unregisters a client and rebalances the remaining clients
// so they can use the freed bandwidth.
func (bm *BandwidthManager) RemoveClient(cl *ClientLimiter) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	for i, c := range bm.clients {
		if c == cl {
			bm.clients = append(bm.clients[:i], bm.clients[i+1:]...)
			break
		}
	}
	bm.rebalanceLocked()
}

// ClientCount returns the current number of registered clients.
func (bm *BandwidthManager) ClientCount() int {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	return len(bm.clients)
}

// rebalanceLocked recalculates and applies per-client rate limits.
// Must be called with bm.mu held.
func (bm *BandwidthManager) rebalanceLocked() {
	n := len(bm.clients)
	if n == 0 {
		return
	}

	perClientBytesPerSec := bm.globalBytesPerSec / int64(n)
	if perClientBytesPerSec <= 0 {
		// If the division results in 0 (more clients than bytes/sec),
		// give each client at least 1 byte/sec to avoid stalling completely.
		perClientBytesPerSec = 1
	}

	for _, cl := range bm.clients {
		cl.setRate(perClientBytesPerSec)
	}
}

// ClientLimiter applies a rate limit to a single client's read and write
// streams. The rate is dynamically adjusted by BandwidthManager as clients
// join and leave.
type ClientLimiter struct {
	mu      sync.RWMutex
	limiter *rate.Limiter
}

// setRate updates the client's rate limit. Called by BandwidthManager
// during rebalancing.
func (cl *ClientLimiter) setRate(bytesPerSec int64) {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	burst := int(bytesPerSec)
	if burst > DefaultBurstSize {
		burst = DefaultBurstSize
	}
	if burst <= 0 {
		burst = 1
	}

	if cl.limiter == nil {
		cl.limiter = rate.NewLimiter(rate.Limit(bytesPerSec), burst)
	} else {
		cl.limiter.SetLimit(rate.Limit(bytesPerSec))
		cl.limiter.SetBurst(burst)
	}
}

// LimitReader wraps an io.Reader so that reads are rate-limited.
func (cl *ClientLimiter) LimitReader(r io.Reader) io.Reader {
	return &limitedReader{reader: r, limiter: cl}
}

// LimitWriter wraps an io.Writer so that writes are rate-limited.
func (cl *ClientLimiter) LimitWriter(w io.Writer) io.Writer {
	return &limitedWriter{writer: w, limiter: cl}
}

// waitN blocks until n tokens are available from the rate limiter.
func (cl *ClientLimiter) waitN(n int) error {
	cl.mu.RLock()
	l := cl.limiter
	cl.mu.RUnlock()

	if l == nil {
		return nil
	}

	// rate.Limiter.WaitN requires n <= Burst. If n exceeds the current
	// burst size, we consume it in burst-sized chunks.
	burst := l.Burst()
	for n > 0 {
		take := n
		if take > burst {
			take = burst
		}
		if err := l.WaitN(context.Background(), take); err != nil {
			return err
		}
		n -= take
	}
	return nil
}

// limitedReader wraps an io.Reader with rate limiting.
type limitedReader struct {
	reader  io.Reader
	limiter *ClientLimiter
}

func (lr *limitedReader) Read(p []byte) (int, error) {
	n, err := lr.reader.Read(p)
	if n > 0 {
		if waitErr := lr.limiter.waitN(n); waitErr != nil {
			return n, waitErr
		}
	}
	return n, err
}

// limitedWriter wraps an io.Writer with rate limiting.
type limitedWriter struct {
	writer  io.Writer
	limiter *ClientLimiter
}

func (lw *limitedWriter) Write(p []byte) (int, error) {
	if err := lw.limiter.waitN(len(p)); err != nil {
		return 0, err
	}
	return lw.writer.Write(p)
}
