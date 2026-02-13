package snowflake_proxy

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	// metricNamespace represent prometheus namespace
	metricNamespace = "tor_snowflake_proxy"
)

// MetricsSources provides dynamic values that are scraped on each
// Prometheus collection. Implementations supply current state that cannot
// be tracked via simple counter increments (e.g. quota remaining, NAT type).
type MetricsSources struct {
	// NATType returns the current NAT type string (e.g. "unrestricted").
	NATType func() string
	// BandwidthBytesPerSec returns the configured bandwidth limit (0 = unlimited).
	BandwidthBytesPerSec func() int64
	// TrafficQuotaBytes returns the configured traffic quota (0 = unlimited).
	TrafficQuotaBytes func() int64
	// TrafficUsedBytes returns the total bytes transferred towards the quota.
	TrafficUsedBytes func() int64
}

type Metrics struct {
	// Existing counters
	totalInBoundTraffic    prometheus.Counter
	totalOutBoundTraffic   prometheus.Counter
	totalConnections       *prometheus.CounterVec
	totalFailedConnections prometheus.Counter

	// New gauges
	connectedClients   prometheus.Gauge
	natType            *prometheus.GaugeVec
	uptimeSeconds      prometheus.Gauge
	bandwidthLimit     prometheus.Gauge
	trafficQuota       prometheus.Gauge
	trafficUsed        prometheus.Gauge
	connectionDuration prometheus.Histogram

	startTime time.Time
	sources   *MetricsSources
}

func NewMetrics() *Metrics {
	return &Metrics{
		totalConnections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricNamespace,
			Name:      "connections_total",
			Help:      "The total number of successful connections handled by the snowflake proxy",
		},
			[]string{"country"},
		),
		totalFailedConnections: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricNamespace,
			Name:      "connection_timeouts_total",
			Help:      "The total number of client connection attempts that failed after successful rendezvous. Note that failures can occur for reasons outside of the proxy's control, such as the client's NAT and censorship situation.",
		}),
		totalInBoundTraffic: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricNamespace,
			Name:      "traffic_inbound_bytes_total",
			Help:      "The total in bound traffic by the snowflake proxy (KB)",
		}),
		totalOutBoundTraffic: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricNamespace,
			Name:      "traffic_outbound_bytes_total",
			Help:      "The total out bound traffic by the snowflake proxy (KB)",
		}),
		connectedClients: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Name:      "connected_clients",
			Help:      "The number of currently connected clients",
		}),
		natType: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Name:      "nat_type",
			Help:      "The current NAT type of the proxy (1 = active type, 0 = inactive). Label 'type' is one of: unknown, restricted, unrestricted",
		},
			[]string{"type"},
		),
		uptimeSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Name:      "uptime_seconds",
			Help:      "The number of seconds the proxy has been running",
		}),
		bandwidthLimit: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Name:      "bandwidth_limit_bytes_per_second",
			Help:      "The configured bandwidth limit in bytes per second (0 = unlimited)",
		}),
		trafficQuota: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Name:      "traffic_quota_bytes_total",
			Help:      "The configured traffic quota in bytes (0 = unlimited)",
		}),
		trafficUsed: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Name:      "traffic_quota_used_bytes",
			Help:      "The total bytes transferred towards the traffic quota",
		}),
		connectionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricNamespace,
			Name:      "connection_duration_seconds",
			Help:      "Distribution of client connection durations in seconds",
			Buckets:   []float64{1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600},
		}),
		startTime: time.Now(),
	}
}

// SetSources configures dynamic metric sources that are read on each
// Prometheus scrape. This must be called before Start if dynamic metrics
// (NAT type, bandwidth, quota) are desired.
func (m *Metrics) SetSources(sources *MetricsSources) {
	m.sources = sources
}

// Start registers the metrics collector and serves the Prometheus endpoint.
// It serves on both /metrics (standard) and /internal/metrics (legacy).
func (m *Metrics) Start(addr string) error {
	go func() {
		handler := promhttp.Handler()
		http.Handle("/metrics", handler)
		http.Handle("/internal/metrics", handler)
		if err := http.ListenAndServe(addr, nil); err != nil {
			panic(err)
		}
	}()

	return prometheus.Register(m)
}

func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	m.totalConnections.Collect(ch)
	m.totalFailedConnections.Collect(ch)
	m.totalInBoundTraffic.Collect(ch)
	m.totalOutBoundTraffic.Collect(ch)
	m.connectedClients.Collect(ch)

	// Update dynamic gauges on each scrape
	m.uptimeSeconds.Set(time.Since(m.startTime).Seconds())
	m.uptimeSeconds.Collect(ch)

	if m.sources != nil {
		if m.sources.NATType != nil {
			natType := m.sources.NATType()
			for _, t := range []string{NATUnknown, NATRestricted, NATUnrestricted} {
				if t == natType {
					m.natType.With(prometheus.Labels{"type": t}).Set(1)
				} else {
					m.natType.With(prometheus.Labels{"type": t}).Set(0)
				}
			}
		}
		if m.sources.BandwidthBytesPerSec != nil {
			m.bandwidthLimit.Set(float64(m.sources.BandwidthBytesPerSec()))
		}
		if m.sources.TrafficQuotaBytes != nil {
			quota := m.sources.TrafficQuotaBytes()
			m.trafficQuota.Set(float64(quota))
		}
		if m.sources.TrafficUsedBytes != nil {
			used := m.sources.TrafficUsedBytes()
			m.trafficUsed.Set(float64(used))
		}
	}

	m.natType.Collect(ch)
	m.bandwidthLimit.Collect(ch)
	m.trafficQuota.Collect(ch)
	m.trafficUsed.Collect(ch)
	m.connectionDuration.Collect(ch)
}

func (m *Metrics) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(m, descs)
}

// TrackInBoundTraffic counts the received traffic by the snowflake proxy
func (m *Metrics) TrackInBoundTraffic(value int64) {
	m.totalInBoundTraffic.Add(float64(value))
}

// TrackOutBoundTraffic counts the transmitted traffic by the snowflake proxy
func (m *Metrics) TrackOutBoundTraffic(value int64) {
	m.totalOutBoundTraffic.Add(float64(value))
}

// TrackNewConnection counts the new connections
func (m *Metrics) TrackNewConnection(country string) {
	m.totalConnections.
		With(prometheus.Labels{"country": country}).
		Inc()
}

// TrackFailedConnection counts failed connection attempts
func (m *Metrics) TrackFailedConnection() {
	m.totalFailedConnections.Inc()
}

// TrackClientConnected increments the connected clients gauge
func (m *Metrics) TrackClientConnected() {
	m.connectedClients.Inc()
}

// TrackClientDisconnected decrements the connected clients gauge
func (m *Metrics) TrackClientDisconnected() {
	m.connectedClients.Dec()
}

// ObserveConnectionDuration records a completed connection's duration in the histogram.
func (m *Metrics) ObserveConnectionDuration(seconds float64) {
	m.connectionDuration.Observe(seconds)
}
