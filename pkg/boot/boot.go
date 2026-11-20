// Package boot holds the startup/shutdown boilerplate shared by all services.
package boot

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/orderflow/pkg/events"
	"github.com/example/orderflow/pkg/messaging"
	"github.com/example/orderflow/pkg/metrics"
	"github.com/example/orderflow/pkg/outbox"
	"github.com/example/orderflow/pkg/telemetry"
)

type App struct {
	Pool     *pgxpool.Pool
	Bus      *messaging.Bus
	stopOTel func(context.Context) error
}

func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func Must(err error) {
	if err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

// New initialises telemetry, Postgres (applying schema), NATS, and starts the
// outbox relay. The relay stops when ctx is cancelled.
func New(ctx context.Context, name, schema string) (*App, error) {
	stop, err := telemetry.Init(ctx, name)
	if err != nil {
		return nil, err
	}
	dsn := Env("DATABASE_URL", "")
	if dsn == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := retry(ctx, "apply schema", func() error {
		_, err := pool.Exec(ctx, schema+outbox.Schema)
		return err
	}); err != nil {
		return nil, err
	}

	var bus *messaging.Bus
	if err := retry(ctx, "connect nats", func() (err error) {
		bus, err = messaging.Connect(Env("NATS_URL", "nats://localhost:4222"))
		return
	}); err != nil {
		return nil, err
	}

	metrics.Serve(Env("METRICS_ADDR", ":9090"))

	relay := &outbox.Relay{Pool: pool, Pub: bus, Interval: 200 * time.Millisecond, Batch: 50}
	go relay.Run(ctx)

	return &App{Pool: pool, Bus: bus, stopOTel: stop}, nil
}

// On registers an idempotent event handler: dedup via inbox, run fn in a
// transaction, and let fn emit follow-up events through the outbox.
func (a *App) On(durable, eventType string, fn func(ctx context.Context, tx pgx.Tx, env events.Envelope) error) error {
	return a.Bus.Consume(durable, events.Subject(eventType), func(ctx context.Context, env events.Envelope) error {
		return outbox.HandleOnce(ctx, a.Pool, env.ID, func(tx pgx.Tx) error {
			return fn(ctx, tx, env)
		})
	})
}

func (a *App) Close() {
	a.Bus.Close() // drains in-flight messages
	a.Pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = a.stopOTel(ctx)
}

func retry(ctx context.Context, what string, fn func() error) error {
	var err error
	for i := 0; i < 30; i++ {
		if err = fn(); err == nil {
			return nil
		}
		slog.Warn("startup step failed, retrying", "step", what, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("%s: %w", what, err)
}
