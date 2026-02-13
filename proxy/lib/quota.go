package snowflake_proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// TrafficQuota enforces a total traffic (bytes transferred) limit across the
// lifetime of the proxy. When the quota is exhausted, new connections are
// rejected and active connections are terminated.
//
// State is periodically persisted to a JSON file so that the counter survives
// restarts. This is designed for VPS operators who have a monthly bandwidth
// allocation (e.g., 20 TB) and want the proxy to auto-stop before incurring
// overage charges.
type TrafficQuota struct {
	limitBytes    int64 // total allowed bytes, immutable after creation
	usedBytes     atomic.Int64
	stateFile     string
	exhausted     chan struct{} // closed when quota is reached
	exhaustedOnce sync.Once
	stopFunc      func() // called to shut down the proxy when quota is hit

	persistDone chan struct{}
}

// TrafficQuotaState is the JSON-serializable state written to disk.
type TrafficQuotaState struct {
	UsedBytes  int64  `json:"used_bytes"`
	LimitBytes int64  `json:"limit_bytes"`
	UpdatedAt  string `json:"updated_at"`
}

// NewTrafficQuota creates a TrafficQuota with the given limit in bytes.
// stateFile is the path to persist usage state (empty string disables persistence).
// stopFunc is called once when the quota is exhausted (typically SnowflakeProxy.Stop).
func NewTrafficQuota(limitBytes int64, stateFile string, stopFunc func()) (*TrafficQuota, error) {
	tq := &TrafficQuota{
		limitBytes:  limitBytes,
		stateFile:   stateFile,
		exhausted:   make(chan struct{}),
		stopFunc:    stopFunc,
		persistDone: make(chan struct{}),
	}

	// Load previous state if state file exists
	if stateFile != "" {
		if err := tq.loadState(); err != nil {
			// Non-fatal: start fresh if state file is missing or corrupt
			log.Printf("Traffic quota: could not load state from %s: %v (starting fresh)", stateFile, err)
		}
	}

	// Check if already exhausted from a previous run
	if tq.usedBytes.Load() >= tq.limitBytes {
		tq.triggerExhausted()
	}

	// Start periodic persistence
	if stateFile != "" {
		go tq.persistLoop()
	} else {
		close(tq.persistDone)
	}

	return tq, nil
}

// Allow returns true if the quota has not been exhausted and new connections
// should be accepted.
func (tq *TrafficQuota) Allow() bool {
	select {
	case <-tq.exhausted:
		return false
	default:
		return true
	}
}

// Exhausted returns a channel that is closed when the quota is reached.
func (tq *TrafficQuota) Exhausted() <-chan struct{} {
	return tq.exhausted
}

// UsedBytes returns the current total bytes consumed.
func (tq *TrafficQuota) UsedBytes() int64 {
	return tq.usedBytes.Load()
}

// LimitBytes returns the configured quota limit.
func (tq *TrafficQuota) LimitBytes() int64 {
	return tq.limitBytes
}

// RemainingBytes returns the bytes remaining before quota exhaustion.
func (tq *TrafficQuota) RemainingBytes() int64 {
	remaining := tq.limitBytes - tq.usedBytes.Load()
	if remaining < 0 {
		return 0
	}
	return remaining
}

// addBytes atomically adds n bytes to the usage counter and checks if the
// quota has been exceeded.
func (tq *TrafficQuota) addBytes(n int64) {
	newTotal := tq.usedBytes.Add(n)
	if newTotal >= tq.limitBytes {
		tq.triggerExhausted()
	}
}

// triggerExhausted is called once when the quota is reached.
func (tq *TrafficQuota) triggerExhausted() {
	tq.exhaustedOnce.Do(func() {
		close(tq.exhausted)
		used := tq.usedBytes.Load()
		log.Printf("Traffic quota exhausted: %s used of %s limit. Shutting down proxy.",
			formatBytes(used), formatBytes(tq.limitBytes))

		// Persist final state before stopping
		if tq.stateFile != "" {
			if err := tq.saveState(); err != nil {
				log.Printf("Traffic quota: error saving final state: %v", err)
			}
		}

		if tq.stopFunc != nil {
			tq.stopFunc()
		}
	})
}

// CountingReader wraps an io.Reader and counts bytes read against the quota.
func (tq *TrafficQuota) CountingReader(r io.Reader) io.Reader {
	return &quotaReader{reader: r, quota: tq}
}

// CountingWriter wraps an io.Writer and counts bytes written against the quota.
func (tq *TrafficQuota) CountingWriter(w io.Writer) io.Writer {
	return &quotaWriter{writer: w, quota: tq}
}

// Close stops the persistence loop and saves final state.
func (tq *TrafficQuota) Close() {
	// persistLoop checks tq.exhausted, but we also need to stop it on clean shutdown
	if tq.stateFile != "" {
		_ = tq.saveState()
	}
}

// persistLoop periodically writes the usage state to disk.
func (tq *TrafficQuota) persistLoop() {
	defer close(tq.persistDone)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := tq.saveState(); err != nil {
				log.Printf("Traffic quota: error persisting state: %v", err)
			}
		case <-tq.exhausted:
			return
		}
	}
}

func (tq *TrafficQuota) loadState() error {
	data, err := os.ReadFile(tq.stateFile)
	if err != nil {
		return err
	}

	var state TrafficQuotaState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("invalid state file: %w", err)
	}

	tq.usedBytes.Store(state.UsedBytes)
	log.Printf("Traffic quota: loaded previous state: %s used of %s limit",
		formatBytes(state.UsedBytes), formatBytes(tq.limitBytes))
	return nil
}

func (tq *TrafficQuota) saveState() error {
	state := TrafficQuotaState{
		UsedBytes:  tq.usedBytes.Load(),
		LimitBytes: tq.limitBytes,
		UpdatedAt:  time.Now().UTC().Format(time.RFC3339),
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}

	// Write atomically: write to temp file then rename
	tmpFile := tq.stateFile + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmpFile, tq.stateFile)
}

// ErrQuotaExhausted is returned by counting readers/writers when the traffic
// quota has been exceeded.
var ErrQuotaExhausted = fmt.Errorf("traffic quota exhausted")

type quotaReader struct {
	reader io.Reader
	quota  *TrafficQuota
}

func (qr *quotaReader) Read(p []byte) (int, error) {
	// Check before reading
	if !qr.quota.Allow() {
		return 0, ErrQuotaExhausted
	}

	n, err := qr.reader.Read(p)
	if n > 0 {
		qr.quota.addBytes(int64(n))
	}
	return n, err
}

type quotaWriter struct {
	writer io.Writer
	quota  *TrafficQuota
}

func (qw *quotaWriter) Write(p []byte) (int, error) {
	// Check before writing
	if !qw.quota.Allow() {
		return 0, ErrQuotaExhausted
	}

	n, err := qw.writer.Write(p)
	if n > 0 {
		qw.quota.addBytes(int64(n))
	}
	return n, err
}

// formatBytes formats byte counts into human-readable strings.
func formatBytes(b int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)
	switch {
	case b >= TB:
		return fmt.Sprintf("%.2f TB", float64(b)/float64(TB))
	case b >= GB:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(GB))
	case b >= MB:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(MB))
	case b >= KB:
		return fmt.Sprintf("%.2f KB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
