// Package metrics exposes Prometheus metrics shared by all services.
package metrics

import (
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// EventsProcessed counts consumer outcomes. result: ok | retry | dlq | malformed.
	EventsProcessed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "events_processed_total",
		Help: "Events handled by consumers, by event type and result.",
	}, []string{"event", "result"})

	// OutboxPending is the number of outbox rows not yet published (relay lag).
	OutboxPending = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_pending",
		Help: "Outbox rows waiting to be published.",
	})

	// OrdersFinalised counts orders reaching a terminal state (orders service).
	OrdersFinalised = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "orders_finalised_total",
		Help: "Orders that reached a terminal status.",
	}, []string{"status"})

	// SagaDuration is the time from order creation to its terminal status.
	SagaDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "saga_duration_seconds",
		Help:    "Time from order creation to CONFIRMED/CANCELLED.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
	})
)

func init() {
	prometheus.MustRegister(EventsProcessed, OutboxPending, OrdersFinalised, SagaDuration)
}

// Serve exposes /metrics on addr in the background.
func Serve(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server failed", "err", err)
		}
	}()
}
