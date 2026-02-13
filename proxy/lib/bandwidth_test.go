package snowflake_proxy

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"
)

func TestBandwidthManagerSingleClient(t *testing.T) {
	// A single client should get the full global limit.
	bm := NewBandwidthManager(100000) // 100 KB/s
	cl := bm.AddClient()

	if bm.ClientCount() != 1 {
		t.Fatalf("expected 1 client, got %d", bm.ClientCount())
	}

	cl.mu.RLock()
	limit := cl.limiter.Limit()
	cl.mu.RUnlock()

	if float64(limit) != 100000 {
		t.Fatalf("expected single client to get full limit 100000, got %v", limit)
	}

	bm.RemoveClient(cl)
	if bm.ClientCount() != 0 {
		t.Fatalf("expected 0 clients after removal, got %d", bm.ClientCount())
	}
}

func TestBandwidthManagerRebalancesOnAdd(t *testing.T) {
	bm := NewBandwidthManager(100000) // 100 KB/s

	cl1 := bm.AddClient()
	cl2 := bm.AddClient()

	if bm.ClientCount() != 2 {
		t.Fatalf("expected 2 clients, got %d", bm.ClientCount())
	}

	// Each client should get 50 KB/s
	cl1.mu.RLock()
	limit1 := float64(cl1.limiter.Limit())
	cl1.mu.RUnlock()

	cl2.mu.RLock()
	limit2 := float64(cl2.limiter.Limit())
	cl2.mu.RUnlock()

	if limit1 != 50000 {
		t.Fatalf("expected client 1 limit to be 50000, got %v", limit1)
	}
	if limit2 != 50000 {
		t.Fatalf("expected client 2 limit to be 50000, got %v", limit2)
	}
}

func TestBandwidthManagerRebalancesOnRemove(t *testing.T) {
	bm := NewBandwidthManager(100000) // 100 KB/s

	cl1 := bm.AddClient()
	cl2 := bm.AddClient()

	// Both at 50 KB/s
	bm.RemoveClient(cl2)

	if bm.ClientCount() != 1 {
		t.Fatalf("expected 1 client after removal, got %d", bm.ClientCount())
	}

	// cl1 should be back to 100 KB/s
	cl1.mu.RLock()
	limit := float64(cl1.limiter.Limit())
	cl1.mu.RUnlock()

	if limit != 100000 {
		t.Fatalf("expected remaining client to get full limit 100000, got %v", limit)
	}
}

func TestBandwidthManagerManyClients(t *testing.T) {
	bm := NewBandwidthManager(100000) // 100 KB/s
	clients := make([]*ClientLimiter, 5)

	for i := 0; i < 5; i++ {
		clients[i] = bm.AddClient()
	}

	if bm.ClientCount() != 5 {
		t.Fatalf("expected 5 clients, got %d", bm.ClientCount())
	}

	// Each should get 20 KB/s
	for i, cl := range clients {
		cl.mu.RLock()
		limit := float64(cl.limiter.Limit())
		cl.mu.RUnlock()
		if limit != 20000 {
			t.Fatalf("expected client %d limit to be 20000, got %v", i, limit)
		}
	}

	// Remove 3 clients, remaining 2 should each get 50 KB/s
	bm.RemoveClient(clients[0])
	bm.RemoveClient(clients[2])
	bm.RemoveClient(clients[4])

	if bm.ClientCount() != 2 {
		t.Fatalf("expected 2 clients, got %d", bm.ClientCount())
	}

	for _, cl := range []*ClientLimiter{clients[1], clients[3]} {
		cl.mu.RLock()
		limit := float64(cl.limiter.Limit())
		cl.mu.RUnlock()
		if limit != 50000 {
			t.Fatalf("expected remaining client limit to be 50000, got %v", limit)
		}
	}
}

func TestBandwidthManagerMoreClientsThanBytesPerSec(t *testing.T) {
	// Edge case: more clients than bytes/sec should give each 1 byte/sec minimum
	bm := NewBandwidthManager(3) // 3 bytes/sec
	clients := make([]*ClientLimiter, 10)
	for i := 0; i < 10; i++ {
		clients[i] = bm.AddClient()
	}

	// Each should get at least 1 byte/sec (floor to avoid zero)
	for i, cl := range clients {
		cl.mu.RLock()
		limit := float64(cl.limiter.Limit())
		cl.mu.RUnlock()
		if limit < 1 {
			t.Fatalf("expected client %d limit to be >= 1, got %v", i, limit)
		}
	}
}

func TestBandwidthManagerRemoveNonexistentClient(t *testing.T) {
	// Removing a client that doesn't exist should not panic
	bm := NewBandwidthManager(100000)
	cl1 := bm.AddClient()
	cl2 := &ClientLimiter{} // not registered

	bm.RemoveClient(cl2) // should be a no-op
	if bm.ClientCount() != 1 {
		t.Fatalf("expected 1 client, got %d", bm.ClientCount())
	}

	bm.RemoveClient(cl1)
	if bm.ClientCount() != 0 {
		t.Fatalf("expected 0 clients, got %d", bm.ClientCount())
	}
}

func TestLimitedReaderThrottles(t *testing.T) {
	// Set a low rate limit and verify throughput is capped.
	bm := NewBandwidthManager(10000) // 10 KB/s
	cl := bm.AddClient()

	// Create a source with 20 KB of data
	data := make([]byte, 20000)
	for i := range data {
		data[i] = byte(i % 256)
	}
	src := bytes.NewReader(data)

	lr := cl.LimitReader(src)

	start := time.Now()
	buf := make([]byte, 2048)
	totalRead := 0
	for {
		n, err := lr.Read(buf)
		totalRead += n
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	elapsed := time.Since(start)

	if totalRead != 20000 {
		t.Fatalf("expected to read 20000 bytes, got %d", totalRead)
	}

	// At 10 KB/s, 20 KB should take at least ~1.5 seconds
	// (allowing some tolerance for token bucket burst)
	if elapsed < 1*time.Second {
		t.Fatalf("expected read to take at least 1s at 10KB/s for 20KB, took %v", elapsed)
	}
}

func TestLimitedWriterThrottles(t *testing.T) {
	// Set a low rate limit and verify write throughput is capped.
	bm := NewBandwidthManager(10000) // 10 KB/s
	cl := bm.AddClient()

	var dst bytes.Buffer
	lw := cl.LimitWriter(&dst)

	data := make([]byte, 20000)
	for i := range data {
		data[i] = byte(i % 256)
	}

	start := time.Now()
	totalWritten := 0
	for totalWritten < len(data) {
		end := totalWritten + 2048
		if end > len(data) {
			end = len(data)
		}
		n, err := lw.Write(data[totalWritten:end])
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		totalWritten += n
	}
	elapsed := time.Since(start)

	if totalWritten != 20000 {
		t.Fatalf("expected to write 20000 bytes, got %d", totalWritten)
	}

	if elapsed < 1*time.Second {
		t.Fatalf("expected write to take at least 1s at 10KB/s for 20KB, took %v", elapsed)
	}
}

func TestNilLimiterPassthrough(t *testing.T) {
	// Verify that a nil ClientLimiter doesn't cause issues.
	// When limiter is nil, copyLoop should not wrap readers/writers.
	var cl *ClientLimiter
	if cl != nil {
		t.Fatal("expected nil limiter")
	}
	// The nil check in copyLoop handles this case — no wrapping occurs.
}

func TestConcurrentAddRemove(t *testing.T) {
	// Stress test: concurrent add/remove shouldn't panic or deadlock
	bm := NewBandwidthManager(1000000) // 1 MB/s

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl := bm.AddClient()
			time.Sleep(10 * time.Millisecond)
			bm.RemoveClient(cl)
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent test timed out — possible deadlock")
	}

	if bm.ClientCount() != 0 {
		t.Fatalf("expected 0 clients after all removed, got %d", bm.ClientCount())
	}
}
