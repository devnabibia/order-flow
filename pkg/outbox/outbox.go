// Package outbox implements the transactional outbox (producer side) and
// inbox (idempotent consumer side) patterns on top of Postgres.
package outbox

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/example/orderflow/pkg/events"
	"github.com/example/orderflow/pkg/metrics"
)

// Schema must be applied by every service that uses this package.
const Schema = `
CREATE TABLE IF NOT EXISTS outbox (
	id         UUID PRIMARY KEY,
	subject    TEXT NOT NULL,
	body       BYTEA NOT NULL,
	headers    JSONB NOT NULL DEFAULT '{}',
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	sent_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS outbox_unsent ON outbox (created_at) WHERE sent_at IS NULL;

CREATE TABLE IF NOT EXISTS inbox (
	message_id   UUID PRIMARY KEY,
	processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
`

// Emit stores an event in the outbox inside the caller's transaction, so the
// state change and the event commit (or roll back) together. The current trace
// context is saved so the relay can continue the trace later.
func Emit(ctx context.Context, tx pgx.Tx, eventType string, payload any) error {
	env, err := events.New(eventType, payload)
	if err != nil {
		return err
	}
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	hdr, err := json.Marshal(carrier)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO outbox (id, subject, body, headers) VALUES ($1, $2, $3, $4)`,
		env.ID, events.Subject(eventType), body, hdr)
	return err
}

// HandleOnce runs fn in a transaction at most once per message ID. The inbox
// insert and fn's writes commit atomically, so redelivered duplicates are no-ops.
func HandleOnce(ctx context.Context, pool *pgxpool.Pool, msgID string, fn func(tx pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `INSERT INTO inbox (message_id) VALUES ($1) ON CONFLICT DO NOTHING`, msgID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		slog.InfoContext(ctx, "duplicate message ignored", "id", msgID)
		return nil
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type Publisher interface {
	Publish(ctx context.Context, id, subject string, body []byte) error
}

// Relay polls the outbox and publishes unsent rows. Delivery is at-least-once:
// if we crash after publishing but before commit, the row is sent again (the
// broker dedups by message ID within its window, consumers dedup via inbox).
type Relay struct {
	Pool     *pgxpool.Pool
	Pub      Publisher
	Interval time.Duration
	Batch    int
}

func (r *Relay) Run(ctx context.Context) {
	t := time.NewTicker(r.Interval)
	defer t.Stop()
	for n := 0; ; n++ {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := r.flush(ctx); err != nil && ctx.Err() == nil {
				slog.ErrorContext(ctx, "outbox flush failed", "err", err)
			}
			if n%5 == 0 { // update the lag gauge about once a second
				var pending int64
				if err := r.Pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE sent_at IS NULL`).Scan(&pending); err == nil {
					metrics.OutboxPending.Set(float64(pending))
				}
			}
		}
	}
}

type row struct {
	id, subject   string
	body, headers []byte
}

func (r *Relay) flush(ctx context.Context) error {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id::text, subject, body, headers FROM outbox
		WHERE sent_at IS NULL ORDER BY created_at
		LIMIT $1 FOR UPDATE SKIP LOCKED`, r.Batch)
	if err != nil {
		return err
	}
	batch, err := pgx.CollectRows(rows, func(rw pgx.CollectableRow) (row, error) {
		var o row
		err := rw.Scan(&o.id, &o.subject, &o.body, &o.headers)
		return o, err
	})
	if err != nil {
		return err
	}

	for _, o := range batch {
		carrier := propagation.MapCarrier{}
		_ = json.Unmarshal(o.headers, &carrier)
		pctx := otel.GetTextMapPropagator().Extract(ctx, carrier)
		if err := r.Pub.Publish(pctx, o.id, o.subject, o.body); err != nil {
			return err // rollback: nothing marked sent, retry next tick
		}
		if _, err := tx.Exec(ctx, `UPDATE outbox SET sent_at = now() WHERE id = $1`, o.id); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
