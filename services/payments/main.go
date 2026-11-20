package main

import (
	"context"
	"errors"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/example/orderflow/pkg/boot"
	"github.com/example/orderflow/pkg/events"
	"github.com/example/orderflow/pkg/outbox"
)

const schema = `
CREATE TABLE IF NOT EXISTS payments (
	order_id     UUID PRIMARY KEY,
	amount_cents BIGINT NOT NULL,
	status       TEXT NOT NULL, -- SUCCEEDED | FAILED | REFUNDED | CANCELLED (tombstone)
	created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
`

var (
	tracer      = otel.Tracer("orderflow/payments")
	errDeclined = errors.New("card declined")
)

const declineAboveCents = 100_000 // fake provider declines anything over $1000

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	app, err := boot.New(ctx, "payments", schema)
	boot.Must(err)
	defer app.Close()

	boot.Must(app.On("payments-inventory-reserved", events.TypeInventoryReserved, onInventoryReserved))
	boot.Must(app.On("payments-order-cancelled", events.TypeOrderCancelled, onOrderCancelled))

	<-ctx.Done()
}

func onInventoryReserved(ctx context.Context, tx pgx.Tx, env events.Envelope) error {
	r, err := events.Decode[events.InventoryReserved](env)
	if err != nil {
		return err
	}

	// Skip if we already handled this order, or if its cancellation arrived first
	// (tombstone): never charge a customer for an order that is already dead.
	var existing string
	err = tx.QueryRow(ctx, `SELECT status FROM payments WHERE order_id = $1 FOR UPDATE`, r.OrderID).Scan(&existing)
	if err == nil {
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	err = charge(ctx, r.OrderID, r.TotalCents)
	switch {
	case err == nil:
		if _, err := tx.Exec(ctx,
			`INSERT INTO payments (order_id, amount_cents, status) VALUES ($1, $2, 'SUCCEEDED') ON CONFLICT DO NOTHING`,
			r.OrderID, r.TotalCents); err != nil {
			return err
		}
		return outbox.Emit(ctx, tx, events.TypePaymentSucceeded, events.PaymentSucceeded{OrderID: r.OrderID})

	case errors.Is(err, errDeclined): // business failure: record it and start compensation
		if _, err := tx.Exec(ctx,
			`INSERT INTO payments (order_id, amount_cents, status) VALUES ($1, $2, 'FAILED') ON CONFLICT DO NOTHING`,
			r.OrderID, r.TotalCents); err != nil {
			return err
		}
		return outbox.Emit(ctx, tx, events.TypePaymentFailed, events.PaymentFailed{OrderID: r.OrderID, Reason: err.Error()})

	default: // transient failure: return the error so the broker redelivers
		return err
	}
}

// onOrderCancelled is the compensating action: refund a successful charge.
func onOrderCancelled(ctx context.Context, tx pgx.Tx, env events.Envelope) error {
	c, err := events.Decode[events.OrderCancelled](env)
	if err != nil {
		return err
	}
	var status string
	err = tx.QueryRow(ctx, `SELECT status FROM payments WHERE order_id = $1 FOR UPDATE`, c.OrderID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		// Cancellation overtook inventory.reserved: leave a tombstone so we never charge later.
		_, err = tx.Exec(ctx, `INSERT INTO payments (order_id, amount_cents, status) VALUES ($1, 0, 'CANCELLED')`, c.OrderID)
		return err
	}
	if err != nil {
		return err
	}
	if status != "SUCCEEDED" {
		return nil // FAILED / already refunded / tombstone: nothing to refund
	}
	// A real provider call goes here, with c.OrderID as the idempotency key.
	if _, err := tx.Exec(ctx, `UPDATE payments SET status = 'REFUNDED' WHERE order_id = $1`, c.OrderID); err != nil {
		return err
	}
	return outbox.Emit(ctx, tx, events.TypePaymentRefunded, events.PaymentRefunded{OrderID: c.OrderID})
}

// charge simulates a payment provider. A real integration must pass orderID as
// the provider's idempotency key so a retry can never double-charge.
func charge(ctx context.Context, orderID string, cents int64) error {
	_, span := tracer.Start(ctx, "provider.charge")
	defer span.End()
	span.SetAttributes(attribute.String("order.id", orderID), attribute.Int64("amount.cents", cents))

	select {
	case <-time.After(200 * time.Millisecond): // simulated latency
	case <-ctx.Done():
		return ctx.Err()
	}
	if cents > declineAboveCents {
		span.RecordError(errDeclined)
		return errDeclined
	}
	return nil
}
