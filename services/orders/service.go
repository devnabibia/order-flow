package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/orderflow/pkg/events"
	"github.com/example/orderflow/pkg/metrics"
	"github.com/example/orderflow/pkg/outbox"
)

const schema = `
CREATE TABLE IF NOT EXISTS orders (
	id              UUID PRIMARY KEY,
	customer_id     TEXT NOT NULL,
	items           JSONB NOT NULL,
	total_cents     BIGINT NOT NULL,
	status          TEXT NOT NULL,
	idempotency_key TEXT UNIQUE,
	created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS orders_created ON orders (created_at DESC);
`

var (
	ErrInvalid  = errors.New("invalid order")
	ErrNotFound = errors.New("order not found")
)

type Order struct {
	ID         string        `json:"id"`
	CustomerID string        `json:"customer_id"`
	Status     string        `json:"status"`
	TotalCents int64         `json:"total_cents"`
	Items      []events.Item `json:"items"`
	CreatedAt  time.Time     `json:"created_at"`
}

// Service holds the orders business logic; HTTP and gRPC are thin adapters over it.
type Service struct{ pool *pgxpool.Pool }

// Create inserts a PENDING order and its order.created event in one transaction.
// With a non-empty key it is idempotent: a repeated key returns the original
// order and created=false.
func (s *Service) Create(ctx context.Context, customerID string, items []events.Item, key string) (id, status string, created bool, err error) {
	if customerID == "" || len(items) == 0 {
		return "", "", false, fmt.Errorf("%w: customer_id and items are required", ErrInvalid)
	}
	var total int64
	for _, it := range items {
		if it.SKU == "" || it.Quantity <= 0 || it.UnitPriceCents < 0 {
			return "", "", false, fmt.Errorf("%w: bad item %q", ErrInvalid, it.SKU)
		}
		total += int64(it.Quantity) * it.UnitPriceCents // demo only: never trust client prices in real life
	}
	itemsJSON, err := json.Marshal(items)
	if err != nil {
		return "", "", false, err
	}
	var keyArg *string // nil => NULL => never conflicts
	if key != "" {
		keyArg = &key
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", "", false, err
	}
	defer tx.Rollback(ctx)

	id = uuid.NewString()
	tag, err := tx.Exec(ctx, `
		INSERT INTO orders (id, customer_id, items, total_cents, status, idempotency_key)
		VALUES ($1, $2, $3, $4, 'PENDING', $5)
		ON CONFLICT (idempotency_key) DO NOTHING`,
		id, customerID, itemsJSON, total, keyArg)
	if err != nil {
		return "", "", false, err
	}
	if tag.RowsAffected() == 0 { // key seen before: hand back the original order
		err := tx.QueryRow(ctx, `SELECT id::text, status FROM orders WHERE idempotency_key = $1`, key).Scan(&id, &status)
		return id, status, false, err
	}

	if err := outbox.Emit(ctx, tx, events.TypeOrderCreated, events.OrderCreated{
		OrderID: id, CustomerID: customerID, Items: items, TotalCents: total,
	}); err != nil {
		return "", "", false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", "", false, err
	}
	return id, "PENDING", true, nil
}

const orderCols = `id::text, customer_id, status, total_cents, items, created_at`

func scanOrder(row pgx.Row) (Order, error) {
	var (
		o     Order
		items []byte
	)
	if err := row.Scan(&o.ID, &o.CustomerID, &o.Status, &o.TotalCents, &items, &o.CreatedAt); err != nil {
		return Order{}, err
	}
	return o, json.Unmarshal(items, &o.Items)
}

func (s *Service) Get(ctx context.Context, id string) (Order, error) {
	if _, err := uuid.Parse(id); err != nil {
		return Order{}, ErrNotFound
	}
	o, err := scanOrder(s.pool.QueryRow(ctx, `SELECT `+orderCols+` FROM orders WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Order{}, ErrNotFound
	}
	return o, err
}

func (s *Service) List(ctx context.Context, limit int) ([]Order, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+orderCols+` FROM orders ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Order, error) { return scanOrder(r) })
}

// ---------- Saga reactions ----------

func (s *Service) onPaymentSucceeded(ctx context.Context, tx pgx.Tx, env events.Envelope) error {
	p, err := events.Decode[events.PaymentSucceeded](env)
	if err != nil {
		return err
	}
	return finalise(ctx, tx, p.OrderID, "CONFIRMED", "")
}

func (s *Service) onPaymentFailed(ctx context.Context, tx pgx.Tx, env events.Envelope) error {
	p, err := events.Decode[events.PaymentFailed](env)
	if err != nil {
		return err
	}
	return finalise(ctx, tx, p.OrderID, "CANCELLED", "payment failed: "+p.Reason)
}

func (s *Service) onInventoryRejected(ctx context.Context, tx pgx.Tx, env events.Envelope) error {
	p, err := events.Decode[events.InventoryRejected](env)
	if err != nil {
		return err
	}
	return finalise(ctx, tx, p.OrderID, "CANCELLED", "inventory rejected: "+p.Reason)
}

// finalise moves a PENDING order to its terminal status. The status guard makes
// late, duplicate, or out-of-order events harmless. Cancellation also emits
// order.cancelled, which triggers compensation (stock release, refund).
func finalise(ctx context.Context, tx pgx.Tx, orderID, status, reason string) error {
	var secs float64
	err := tx.QueryRow(ctx, `
		UPDATE orders SET status = $2 WHERE id = $1 AND status = 'PENDING'
		RETURNING EXTRACT(EPOCH FROM now() - created_at)::float8`, orderID, status).Scan(&secs)
	if errors.Is(err, pgx.ErrNoRows) {
		slog.InfoContext(ctx, "order already finalised, ignoring", "order_id", orderID, "attempted", status)
		return nil
	}
	if err != nil {
		return err
	}
	metrics.OrdersFinalised.WithLabelValues(status).Inc()
	metrics.SagaDuration.Observe(secs)

	if status == "CANCELLED" {
		return outbox.Emit(ctx, tx, events.TypeOrderCancelled, events.OrderCancelled{OrderID: orderID, Reason: reason})
	}
	return nil
}

// reapStale cancels orders stuck in PENDING (lost event, crashed service...).
func (s *Service) reapStale(ctx context.Context) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.reapOnce(ctx); err != nil && ctx.Err() == nil {
				slog.ErrorContext(ctx, "reaper failed", "err", err)
			}
		}
	}
}

func (s *Service) reapOnce(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id::text FROM orders
		WHERE status = 'PENDING' AND created_at < now() - interval '60 seconds'
		FOR UPDATE SKIP LOCKED`)
	if err != nil {
		return err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := finalise(ctx, tx, id, "CANCELLED", "timeout"); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
