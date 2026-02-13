package snowflake_proxy

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// collectMetric is a helper that collects metrics from a collector and returns them as a map.
func collectMetric(c prometheus.Collector) []*dto.MetricFamily {
	ch := make(chan prometheus.Metric, 100)
	c.Collect(ch)
	close(ch)

	var metrics []prometheus.Metric
	for m := range ch {
		metrics = append(metrics, m)
	}

	// Convert to MetricFamily via a gatherer
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(c)
	families, _ := reg.Gather()
	return families
}

func findFamily(families []*dto.MetricFamily, name string) *dto.MetricFamily {
	for _, f := range families {
		if f.GetName() == name {
			return f
		}
	}
	return nil
}

func TestNewMetrics(t *testing.T) {
	m := NewMetrics()
	if m == nil {
		t.Fatal("NewMetrics() returned nil")
	}
	if m.startTime.IsZero() {
		t.Error("startTime should be set")
	}
}

func TestTrackInBoundTraffic(t *testing.T) {
	m := NewMetrics()
	m.TrackInBoundTraffic(1024)
	m.TrackInBoundTraffic(2048)

	families := collectMetric(m)
	f := findFamily(families, "tor_snowflake_proxy_traffic_inbound_bytes_total")
	if f == nil {
		t.Fatal("inbound traffic metric not found")
	}
	val := f.GetMetric()[0].GetCounter().GetValue()
	if val != 3072 {
		t.Errorf("expected 3072, got %f", val)
	}
}

func TestTrackOutBoundTraffic(t *testing.T) {
	m := NewMetrics()
	m.TrackOutBoundTraffic(500)
	m.TrackOutBoundTraffic(500)

	families := collectMetric(m)
	f := findFamily(families, "tor_snowflake_proxy_traffic_outbound_bytes_total")
	if f == nil {
		t.Fatal("outbound traffic metric not found")
	}
	val := f.GetMetric()[0].GetCounter().GetValue()
	if val != 1000 {
		t.Errorf("expected 1000, got %f", val)
	}
}

func TestTrackNewConnection(t *testing.T) {
	m := NewMetrics()
	m.TrackNewConnection("US")
	m.TrackNewConnection("US")
	m.TrackNewConnection("IR")

	families := collectMetric(m)
	f := findFamily(families, "tor_snowflake_proxy_connections_total")
	if f == nil {
		t.Fatal("connections metric not found")
	}
	// Should have 2 label sets
	if len(f.GetMetric()) != 2 {
		t.Errorf("expected 2 label sets, got %d", len(f.GetMetric()))
	}
	for _, metric := range f.GetMetric() {
		country := metric.GetLabel()[0].GetValue()
		val := metric.GetCounter().GetValue()
		switch country {
		case "US":
			if val != 2 {
				t.Errorf("US connections: expected 2, got %f", val)
			}
		case "IR":
			if val != 1 {
				t.Errorf("IR connections: expected 1, got %f", val)
			}
		}
	}
}

func TestTrackFailedConnection(t *testing.T) {
	m := NewMetrics()
	m.TrackFailedConnection()
	m.TrackFailedConnection()
	m.TrackFailedConnection()

	families := collectMetric(m)
	f := findFamily(families, "tor_snowflake_proxy_connection_timeouts_total")
	if f == nil {
		t.Fatal("failed connections metric not found")
	}
	val := f.GetMetric()[0].GetCounter().GetValue()
	if val != 3 {
		t.Errorf("expected 3, got %f", val)
	}
}

func TestTrackClientConnectedDisconnected(t *testing.T) {
	m := NewMetrics()

	// Connect 3 clients
	m.TrackClientConnected()
	m.TrackClientConnected()
	m.TrackClientConnected()

	// Disconnect 1
	m.TrackClientDisconnected()

	families := collectMetric(m)
	f := findFamily(families, "tor_snowflake_proxy_connected_clients")
	if f == nil {
		t.Fatal("connected clients metric not found")
	}
	val := f.GetMetric()[0].GetGauge().GetValue()
	if val != 2 {
		t.Errorf("expected 2 connected clients, got %f", val)
	}
}

func TestUptimeSeconds(t *testing.T) {
	m := NewMetrics()

	families := collectMetric(m)
	f := findFamily(families, "tor_snowflake_proxy_uptime_seconds")
	if f == nil {
		t.Fatal("uptime metric not found")
	}
	val := f.GetMetric()[0].GetGauge().GetValue()
	if val < 0 {
		t.Errorf("uptime should be non-negative, got %f", val)
	}
}

func TestDynamicSourcesNATType(t *testing.T) {
	m := NewMetrics()
	m.SetSources(&MetricsSources{
		NATType: func() string { return NATRestricted },
	})

	families := collectMetric(m)
	f := findFamily(families, "tor_snowflake_proxy_nat_type")
	if f == nil {
		t.Fatal("nat_type metric not found")
	}

	for _, metric := range f.GetMetric() {
		label := metric.GetLabel()[0].GetValue()
		val := metric.GetGauge().GetValue()
		if label == NATRestricted && val != 1 {
			t.Errorf("NAT type %q should be 1, got %f", label, val)
		}
		if label != NATRestricted && val != 0 {
			t.Errorf("NAT type %q should be 0, got %f", label, val)
		}
	}
}

func TestDynamicSourcesBandwidth(t *testing.T) {
	m := NewMetrics()
	m.SetSources(&MetricsSources{
		BandwidthBytesPerSec: func() int64 { return 1_000_000 },
	})

	families := collectMetric(m)
	f := findFamily(families, "tor_snowflake_proxy_bandwidth_limit_bytes_per_second")
	if f == nil {
		t.Fatal("bandwidth limit metric not found")
	}
	val := f.GetMetric()[0].GetGauge().GetValue()
	if val != 1_000_000 {
		t.Errorf("expected 1000000, got %f", val)
	}
}

func TestDynamicSourcesTrafficQuota(t *testing.T) {
	m := NewMetrics()
	m.SetSources(&MetricsSources{
		TrafficQuotaBytes: func() int64 { return 10_000_000 },
		TrafficUsedBytes:  func() int64 { return 3_000_000 },
	})

	families := collectMetric(m)

	// Check quota total
	f := findFamily(families, "tor_snowflake_proxy_traffic_quota_bytes_total")
	if f == nil {
		t.Fatal("traffic quota metric not found")
	}
	if val := f.GetMetric()[0].GetGauge().GetValue(); val != 10_000_000 {
		t.Errorf("quota: expected 10000000, got %f", val)
	}

	// Check used
	f = findFamily(families, "tor_snowflake_proxy_traffic_quota_used_bytes")
	if f == nil {
		t.Fatal("traffic used metric not found")
	}
	if val := f.GetMetric()[0].GetGauge().GetValue(); val != 3_000_000 {
		t.Errorf("used: expected 3000000, got %f", val)
	}

	// Remaining is NOT a metric — it's derivable from total - used
	f = findFamily(families, "tor_snowflake_proxy_traffic_quota_remaining_bytes")
	if f != nil {
		t.Error("traffic_quota_remaining_bytes should not exist (redundant with total - used)")
	}
}

func TestDynamicSourcesNoSources(t *testing.T) {
	m := NewMetrics()
	// No sources set — should not panic on collect
	families := collectMetric(m)
	if families == nil {
		t.Fatal("expected non-nil families even without sources")
	}
}

func TestMetricsImplementsEventCollector(t *testing.T) {
	// Ensure Metrics satisfies the EventCollector interface at compile time
	var _ EventCollector = (*Metrics)(nil)
}

func TestGetTrafficQuotaNil(t *testing.T) {
	sf := &SnowflakeProxy{}
	if sf.GetTrafficQuota() != nil {
		t.Error("expected nil when traffic quota is not set")
	}
}

func TestObserveConnectionDuration(t *testing.T) {
	m := NewMetrics()
	m.ObserveConnectionDuration(0.5)  // < 1s bucket
	m.ObserveConnectionDuration(3.0)  // 1-5s bucket
	m.ObserveConnectionDuration(45.0) // 30-60s bucket
	m.ObserveConnectionDuration(7200) // > 3600s, +Inf bucket

	families := collectMetric(m)
	f := findFamily(families, "tor_snowflake_proxy_connection_duration_seconds")
	if f == nil {
		t.Fatal("connection duration histogram not found")
	}
	h := f.GetMetric()[0].GetHistogram()
	if h.GetSampleCount() != 4 {
		t.Errorf("expected 4 observations, got %d", h.GetSampleCount())
	}
	// Sum should be 0.5 + 3.0 + 45.0 + 7200 = 7248.5
	if h.GetSampleSum() != 7248.5 {
		t.Errorf("expected sum 7248.5, got %f", h.GetSampleSum())
	}
}

func TestConnectionDurationBuckets(t *testing.T) {
	m := NewMetrics()
	// All under 1 second
	m.ObserveConnectionDuration(0.1)
	m.ObserveConnectionDuration(0.5)
	m.ObserveConnectionDuration(0.9)

	families := collectMetric(m)
	f := findFamily(families, "tor_snowflake_proxy_connection_duration_seconds")
	if f == nil {
		t.Fatal("connection duration histogram not found")
	}
	h := f.GetMetric()[0].GetHistogram()

	// First bucket (le=1) should contain all 3
	for _, b := range h.GetBucket() {
		if b.GetUpperBound() == 1.0 {
			if b.GetCumulativeCount() != 3 {
				t.Errorf("le=1 bucket: expected 3, got %d", b.GetCumulativeCount())
			}
			break
		}
	}
}
