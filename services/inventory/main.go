package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/example/orderflow/pkg/boot"
	"github.com/example/orderflow/pkg/events"
	"github.com/example/orderflow/pkg/outbox"
)

const schema = `
CREATE TABLE IF NOT EXISTS stock (
	sku      TEXT PRIMARY KEY,
	quantity INT NOT NULL CHECK (quantity >= 0)
);
CREATE TABLE IF NOT EXISTS reservations (
	order_id UUID PRIMARY KEY,
	status   TEXT NOT NULL, -- RESERVED | REJECTED | RELEASED
	items    JSONB NOT NULL
);
-- sku-3 has only 5 units: order more to trigger the rejection path.
INSERT INTO stock (sku, quantity) VALUES ('sku-1', 100), ('sku-2', 100), ('sku-3', 5), ('sku-4', 100)
ON CONFLICT DO NOTHING;
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	app, err := boot.New(ctx, "inventory", schema)
	boot.Must(err)
	defer app.Close()

	boot.Must(app.On("inventory-order-created", events.TypeOrderCreated, onOrderCreated))
	boot.Must(app.On("inventory-order-cancelled", events.TypeOrderCancelled, onOrderCancelled))

	// Read-only view of stock levels, handy for demos and the e2e tests.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /stock", func(w http.ResponseWriter, r *http.Request) {
		rows, err := app.Pool.Query(r.Context(), `SELECT sku, quantity FROM stock ORDER BY sku`)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		type kv struct {
			sku string
			qty int
		}
		list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (kv, error) {
			var x kv
			return x, r.Scan(&x.sku, &x.qty)
		})
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		out := make(map[string]int, len(list))
		for _, x := range list {
			out[x.sku] = x.qty
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	srv := &http.Server{Addr: boot.Env("HTTP_ADDR", ":8081"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		slog.Error("http server failed", "err", err)
	}
}

func onOrderCreated(ctx context.Context, tx pgx.Tx, env events.Envelope) error {
	o, err := events.Decode[events.OrderCreated](env)
	if err != nil {
		return err
	}
	itemsJSON, _ := json.Marshal(o.Items)

	// Claim the order first. If a cancellation already arrived (status RELEASED),
	// the row exists and we must not reserve stock for a dead order.
	tag, err := tx.Exec(ctx,
		`INSERT INTO reservations (order_id, status, items) VALUES ($1, 'RESERVED', $2) ON CONFLICT DO NOTHING`,
		o.OrderID, itemsJSON)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil
	}

	ok, err := reserve(ctx, tx, o.Items)
	if err != nil {
		return err
	}
	if !ok {
		if _, err := tx.Exec(ctx, `UPDATE reservations SET status = 'REJECTED' WHERE order_id = $1`, o.OrderID); err != nil {
			return err
		}
		return outbox.Emit(ctx, tx, events.TypeInventoryRejected,
			events.InventoryRejected{OrderID: o.OrderID, Reason: "insufficient stock"})
	}
	return outbox.Emit(ctx, tx, events.TypeInventoryReserved,
		events.InventoryReserved{OrderID: o.OrderID, CustomerID: o.CustomerID, TotalCents: o.TotalCents})
}

// reserve decrements stock for all items atomically via a savepoint: if any
// item is short, every decrement is rolled back and ok=false is returned.
func reserve(ctx context.Context, tx pgx.Tx, items []events.Item) (bool, error) {
	sorted := append([]events.Item(nil), items...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].SKU < sorted[j].SKU }) // consistent lock order avoids deadlocks

	sp, err := tx.Begin(ctx) // nested Begin == SAVEPOINT
	if err != nil {
		return false, err
	}
	for _, it := range sorted {
		tag, err := sp.Exec(ctx,
			`UPDATE stock SET quantity = quantity - $2 WHERE sku = $1 AND quantity >= $2`, it.SKU, it.Quantity)
		if err != nil {
			_ = sp.Rollback(ctx)
			return false, err
		}
		if tag.RowsAffected() == 0 {
			return false, sp.Rollback(ctx)
		}
	}
	return true, sp.Commit(ctx)
}

// onOrderCancelled is the saga's compensating action: give reserved stock back.
func onOrderCancelled(ctx context.Context, tx pgx.Tx, env events.Envelope) error {
	c, err := events.Decode[events.OrderCancelled](env)
	if err != nil {
		return err
	}
	var status string
	var itemsJSON []byte
	err = tx.QueryRow(ctx, `SELECT status, items FROM reservations WHERE order_id = $1 FOR UPDATE`, c.OrderID).
		Scan(&status, &itemsJSON)
	if errors.Is(err, pgx.ErrNoRows) {
		// Cancellation overtook order.created: leave a tombstone so we never reserve later.
		_, err = tx.Exec(ctx, `INSERT INTO reservations (order_id, status, items) VALUES ($1, 'RELEASED', '[]')`, c.OrderID)
		return err
	}
	if err != nil {
		return err
	}
	if status != "RESERVED" {
		return nil // REJECTED or already RELEASED: nothing to give back
	}

	var items []events.Item
	if err := json.Unmarshal(itemsJSON, &items); err != nil {
		return err
	}
	for _, it := range items {
		if _, err := tx.Exec(ctx, `UPDATE stock SET quantity = quantity + $2 WHERE sku = $1`, it.SKU, it.Quantity); err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `UPDATE reservations SET status = 'RELEASED' WHERE order_id = $1`, c.OrderID)
	return err
}
