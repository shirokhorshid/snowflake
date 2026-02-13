package snowflake_proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestTrafficQuotaBasicAllow(t *testing.T) {
	tq, err := NewTrafficQuota(1000, "", nil)
	if err != nil {
		t.Fatal(err)
	}

	if !tq.Allow() {
		t.Fatal("expected Allow() to return true when quota not exhausted")
	}

	if tq.UsedBytes() != 0 {
		t.Fatalf("expected 0 used bytes, got %d", tq.UsedBytes())
	}

	if tq.RemainingBytes() != 1000 {
		t.Fatalf("expected 1000 remaining, got %d", tq.RemainingBytes())
	}
}

func TestTrafficQuotaExhaustion(t *testing.T) {
	stopCalled := make(chan struct{})
	stopFunc := func() { close(stopCalled) }

	tq, err := NewTrafficQuota(100, "", stopFunc)
	if err != nil {
		t.Fatal(err)
	}

	// Write 100 bytes — should exactly exhaust the quota
	w := tq.CountingWriter(io.Discard)
	data := make([]byte, 100)
	n, err := w.Write(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 100 {
		t.Fatalf("expected 100 bytes written, got %d", n)
	}

	// Wait for stop to be called
	select {
	case <-stopCalled:
		// good
	case <-time.After(2 * time.Second):
		t.Fatal("stopFunc was not called after quota exhaustion")
	}

	if tq.Allow() {
		t.Fatal("expected Allow() to return false after exhaustion")
	}

	if tq.RemainingBytes() != 0 {
		t.Fatalf("expected 0 remaining, got %d", tq.RemainingBytes())
	}
}

func TestTrafficQuotaRejectsWriteAfterExhaustion(t *testing.T) {
	tq, err := NewTrafficQuota(50, "", nil)
	if err != nil {
		t.Fatal(err)
	}

	w := tq.CountingWriter(io.Discard)

	// First write exhausts quota
	_, _ = w.Write(make([]byte, 50))

	// Second write should be rejected
	_, writeErr := w.Write(make([]byte, 10))
	if writeErr != ErrQuotaExhausted {
		t.Fatalf("expected ErrQuotaExhausted, got %v", writeErr)
	}
}

func TestTrafficQuotaRejectsReadAfterExhaustion(t *testing.T) {
	tq, err := NewTrafficQuota(50, "", nil)
	if err != nil {
		t.Fatal(err)
	}

	src := bytes.NewReader(make([]byte, 100))
	r := tq.CountingReader(src)

	// Read 50 bytes to exhaust
	buf := make([]byte, 50)
	n, readErr := r.Read(buf)
	if readErr != nil {
		t.Fatalf("first read should succeed, got error: %v", readErr)
	}
	if n != 50 {
		t.Fatalf("expected 50 bytes read, got %d", n)
	}

	// Next read should be rejected
	_, readErr = r.Read(buf)
	if readErr != ErrQuotaExhausted {
		t.Fatalf("expected ErrQuotaExhausted, got %v", readErr)
	}
}

func TestTrafficQuotaCountsBothDirections(t *testing.T) {
	tq, err := NewTrafficQuota(100, "", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Write 30 bytes
	w := tq.CountingWriter(io.Discard)
	_, _ = w.Write(make([]byte, 30))

	// Read 30 bytes
	src := bytes.NewReader(make([]byte, 30))
	r := tq.CountingReader(src)
	buf := make([]byte, 30)
	_, _ = r.Read(buf)

	// Total should be 60
	if tq.UsedBytes() != 60 {
		t.Fatalf("expected 60 used bytes (30 write + 30 read), got %d", tq.UsedBytes())
	}

	if tq.RemainingBytes() != 40 {
		t.Fatalf("expected 40 remaining, got %d", tq.RemainingBytes())
	}
}

func TestTrafficQuotaStatePersistence(t *testing.T) {
	dir := t.TempDir()
	stateFile := filepath.Join(dir, "quota_state.json")

	// Create quota and use some bytes
	tq1, err := NewTrafficQuota(10000, stateFile, nil)
	if err != nil {
		t.Fatal(err)
	}

	w := tq1.CountingWriter(io.Discard)
	_, _ = w.Write(make([]byte, 3000))

	// Force save
	tq1.Close()

	// Verify state file exists and has correct content
	data, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("state file not found: %v", err)
	}

	var state TrafficQuotaState
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("invalid state JSON: %v", err)
	}

	if state.UsedBytes != 3000 {
		t.Fatalf("expected 3000 used bytes in state, got %d", state.UsedBytes)
	}
	if state.LimitBytes != 10000 {
		t.Fatalf("expected 10000 limit bytes in state, got %d", state.LimitBytes)
	}

	// Create a new quota loading from the same state file
	tq2, err := NewTrafficQuota(10000, stateFile, nil)
	if err != nil {
		t.Fatal(err)
	}

	if tq2.UsedBytes() != 3000 {
		t.Fatalf("expected loaded quota to have 3000 used bytes, got %d", tq2.UsedBytes())
	}

	if tq2.RemainingBytes() != 7000 {
		t.Fatalf("expected 7000 remaining after reload, got %d", tq2.RemainingBytes())
	}

	if !tq2.Allow() {
		t.Fatal("expected Allow() to return true after reload with remaining quota")
	}

	tq2.Close()
}

func TestTrafficQuotaStateResumesExhausted(t *testing.T) {
	dir := t.TempDir()
	stateFile := filepath.Join(dir, "quota_state.json")

	// Create state file with exhausted quota
	state := TrafficQuotaState{
		UsedBytes:  5000,
		LimitBytes: 5000,
		UpdatedAt:  time.Now().UTC().Format(time.RFC3339),
	}
	data, _ := json.Marshal(state)
	os.WriteFile(stateFile, data, 0600)

	stopCalled := make(chan struct{})
	tq, err := NewTrafficQuota(5000, stateFile, func() { close(stopCalled) })
	if err != nil {
		t.Fatal(err)
	}

	if tq.Allow() {
		t.Fatal("expected Allow() to be false when loading exhausted state")
	}

	select {
	case <-stopCalled:
		// good — stop was called immediately on load
	case <-time.After(2 * time.Second):
		t.Fatal("stopFunc was not called for pre-exhausted quota")
	}

	tq.Close()
}

func TestTrafficQuotaMissingStateFile(t *testing.T) {
	// Should start fresh when state file doesn't exist
	tq, err := NewTrafficQuota(10000, "/nonexistent/path/state.json", nil)
	if err != nil {
		t.Fatal(err)
	}

	if tq.UsedBytes() != 0 {
		t.Fatalf("expected 0 used bytes with missing state, got %d", tq.UsedBytes())
	}

	if !tq.Allow() {
		t.Fatal("expected Allow() to be true with fresh start")
	}
}

func TestTrafficQuotaCorruptStateFile(t *testing.T) {
	dir := t.TempDir()
	stateFile := filepath.Join(dir, "quota_state.json")

	// Write garbage
	os.WriteFile(stateFile, []byte("not json"), 0600)

	tq, err := NewTrafficQuota(10000, stateFile, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Should start fresh
	if tq.UsedBytes() != 0 {
		t.Fatalf("expected 0 used bytes with corrupt state, got %d", tq.UsedBytes())
	}
}

func TestTrafficQuotaConcurrentWrites(t *testing.T) {
	tq, err := NewTrafficQuota(1000000, "", nil)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	perGoroutine := 1000
	numGoroutines := 50

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := tq.CountingWriter(io.Discard)
			_, _ = w.Write(make([]byte, perGoroutine))
		}()
	}

	wg.Wait()

	expected := int64(perGoroutine * numGoroutines)
	if tq.UsedBytes() != expected {
		t.Fatalf("expected %d used bytes after concurrent writes, got %d", expected, tq.UsedBytes())
	}
}

func TestTrafficQuotaStopCalledOnce(t *testing.T) {
	callCount := 0
	var mu sync.Mutex
	stopFunc := func() {
		mu.Lock()
		callCount++
		mu.Unlock()
	}

	tq, err := NewTrafficQuota(100, "", stopFunc)
	if err != nil {
		t.Fatal(err)
	}

	// Exhaust the quota from multiple goroutines simultaneously
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := tq.CountingWriter(io.Discard)
			_, _ = w.Write(make([]byte, 50))
		}()
	}
	wg.Wait()

	time.Sleep(100 * time.Millisecond) // let exhaustion trigger propagate

	mu.Lock()
	if callCount != 1 {
		t.Fatalf("expected stopFunc to be called exactly once, got %d", callCount)
	}
	mu.Unlock()
}

func TestTrafficQuotaNoStateFile(t *testing.T) {
	// No state file means no persistence — just counting
	tq, err := NewTrafficQuota(500, "", nil)
	if err != nil {
		t.Fatal(err)
	}

	w := tq.CountingWriter(io.Discard)
	_, _ = w.Write(make([]byte, 200))

	if tq.UsedBytes() != 200 {
		t.Fatalf("expected 200, got %d", tq.UsedBytes())
	}

	tq.Close() // should not panic
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0 B"},
		{500, "500 B"},
		{1024, "1.00 KB"},
		{1536, "1.50 KB"},
		{1048576, "1.00 MB"},
		{1073741824, "1.00 GB"},
		{1099511627776, "1.00 TB"},
		{21990232555520, "20.00 TB"},
	}

	for _, tt := range tests {
		got := formatBytes(tt.input)
		if got != tt.expected {
			t.Errorf("formatBytes(%d) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}
