package main

import (
	"context"
	"errors"
	"net/http"

	"github.com/Waterkyuu/soroi"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// registerMetrics exports cumulative snapshots directly, without lossy diagnostic subscriptions.
func registerMetrics(client *soroi.Client, registry *prometheus.Registry) (*prometheus.HistogramVec, error) {
	metrics := []prometheus.Collector{
		prometheus.NewCounterFunc(
			prometheus.CounterOpts{Name: "soroi_cache_hits_total", Help: "Fresh local cache decisions."},
			func() float64 { return float64(client.Stats().CacheHits) },
		),
		prometheus.NewCounterFunc(
			prometheus.CounterOpts{Name: "soroi_cache_misses_total", Help: "Local decisions requiring a load or join."},
			func() float64 { return float64(client.Stats().CacheMisses) },
		),
		prometheus.NewCounterFunc(
			prometheus.CounterOpts{
				Name: "soroi_loads_total",
				Help: "Logical query loads, including admission failures.",
			},
			func() float64 { return float64(client.Stats().Loads) },
		),
		prometheus.NewCounterFunc(
			prometheus.CounterOpts{
				Name: "soroi_merged_requests_total",
				Help: "Consumers joining existing query loads.",
			},
			func() float64 { return float64(client.Stats().MergedRequests) },
		),
		prometheus.NewCounterFunc(
			prometheus.CounterOpts{Name: "soroi_retries_total", Help: "Additional loader attempts."},
			func() float64 { return float64(client.Stats().Retries) },
		),
		prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{Name: "soroi_cache_entries", Help: "Currently retained query results and errors."},
			func() float64 { return float64(client.Stats().CacheEntries) },
		),
	}
	duration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "soroi_fetch_duration_seconds", Help: "Caller-visible query latency, including joins and cache hits.",
		Buckets: prometheus.DefBuckets,
	}, []string{"outcome"})
	metrics = append(metrics, duration)
	for _, metric := range metrics {
		if err := registry.Register(metric); err != nil {
			return nil, err
		}
	}
	return duration, nil
}

// errorStatus maps query and RPC errors at the HTTP boundary without changing their retry semantics.
func errorStatus(err error) int {
	switch {
	case errors.Is(err, soroi.ErrQueueFull):
		return http.StatusServiceUnavailable
	case errors.Is(err, context.DeadlineExceeded), status.Code(err) == codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	case errors.Is(err, context.Canceled), status.Code(err) == codes.Canceled:
		return http.StatusRequestTimeout
	case status.Code(err) == codes.NotFound:
		return http.StatusNotFound
	default:
		return http.StatusBadGateway
	}
}
