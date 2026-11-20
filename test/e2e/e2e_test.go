//go:build e2e

// End-to-end tests against a running stack:
//
//	docker compose -f deploy/docker-compose.yml up --build -d
//	go test -tags e2e -v ./test/e2e/
//
// Tests run sequentially and each stock-changing test uses its own SKU.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	ordersv1 "github.com/example/orderflow/gen/orders/v1"
	"github.com/example/orderflow/pkg/events"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

var (
	ordersURL    = env("ORDERS_URL", "http://localhost:8080")
	inventoryURL = env("INVENTORY_URL", "http://localhost:8081")
	natsURL      = env("NATS_URL", "nats://localhost:4222")
	grpcAddr     = env("ORDERS_GRPC", "localhost:9000")
)

type item = map[string]any

func createOrder(t *testing.T, key string, items ...item) (id string, code int) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"customer_id": "e2e", "items": items})
	req, _ := http.NewRequest("POST", ordersURL+"/orders", bytes.NewReader(body))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create order: %v (is the stack running?)", err)
	}
	defer resp.Body.Close()
	var out struct {
		ID string `json:"id"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out.ID, resp.StatusCode
}

func orderStatus(t *testing.T, id string) string {
	t.Helper()
	resp, err := http.Get(ordersURL + "/orders/" + id)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var o struct {
		Status string `json:"status"`
	}
	_ = json.Unmarshal(b, &o)
	return o.Status
}

func eventually(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func waitStatus(t *testing.T, id, want string) {
	t.Helper()
	var last string
	eventually(t, fmt.Sprintf("order %s to become %s", id, want), 20*time.Second, func() bool {
		last = orderStatus(t, id)
		return last == want
	})
	_ = last
}

func stock(t *testing.T) map[string]int {
	t.Helper()
	resp, err := http.Get(inventoryURL + "/stock")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	m := map[string]int{}
	_ = json.NewDecoder(resp.Body).Decode(&m)
	return m
}

func TestHappyPath(t *testing.T) {
	before := stock(t)["sku-1"]
	id, code := createOrder(t, "", item{"sku": "sku-1", "quantity": 2, "unit_price_cents": 1500})
	if code != http.StatusCreated {
		t.Fatalf("want 201, got %d", code)
	}
	waitStatus(t, id, "CONFIRMED")
	if got := stock(t)["sku-1"]; got != before-2 {
		t.Fatalf("stock: want %d, got %d", before-2, got)
	}
}

func TestIdempotencyKey(t *testing.T) {
	key := uuid.NewString()
	it := item{"sku": "sku-1", "quantity": 1, "unit_price_cents": 100}
	id1, c1 := createOrder(t, key, it)
	id2, c2 := createOrder(t, key, it)
	if c1 != http.StatusCreated || c2 != http.StatusOK {
		t.Fatalf("want 201 then 200, got %d then %d", c1, c2)
	}
	if id1 != id2 {
		t.Fatalf("same key created two orders: %s vs %s", id1, id2)
	}
	waitStatus(t, id1, "CONFIRMED")
}

func TestOutOfStockCancels(t *testing.T) {
	before := stock(t)["sku-3"]
	id, _ := createOrder(t, "", item{"sku": "sku-3", "quantity": 999, "unit_price_cents": 100})
	waitStatus(t, id, "CANCELLED")
	if got := stock(t)["sku-3"]; got != before {
		t.Fatalf("stock must be untouched: want %d, got %d", before, got)
	}
}

func TestPaymentDeclinedReleasesStock(t *testing.T) {
	before := stock(t)["sku-2"]
	id, _ := createOrder(t, "", item{"sku": "sku-2", "quantity": 1, "unit_price_cents": 200_000})
	waitStatus(t, id, "CANCELLED")
	// Compensation: reservation released after the cancellation event.
	eventually(t, "stock to be released", 15*time.Second, func() bool { return stock(t)["sku-2"] == before })
}

// A duplicate delivery (same envelope ID, different broker message ID) must not
// reserve stock twice: the inbox makes the consumer idempotent.
func TestDuplicateEventIsIdempotent(t *testing.T) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, _ := nc.JetStream()

	before := stock(t)["sku-4"]
	env, _ := events.New(events.TypeOrderCreated, events.OrderCreated{
		OrderID: uuid.NewString(), CustomerID: "dup", TotalCents: 300,
		Items: []events.Item{{SKU: "sku-4", Quantity: 3, UnitPriceCents: 100}},
	})
	body, _ := json.Marshal(env)
	for i := 0; i < 2; i++ {
		msg := nats.NewMsg(events.Subject(events.TypeOrderCreated))
		msg.Data = body
		msg.Header.Set(nats.MsgIdHdr, uuid.NewString()) // defeat broker-side dedup on purpose
		if _, err := js.PublishMsg(msg); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "stock reserved once", 15*time.Second, func() bool { return stock(t)["sku-4"] == before-3 })
	time.Sleep(2 * time.Second) // give a (buggy) second delivery time to land
	if got := stock(t)["sku-4"]; got != before-3 {
		t.Fatalf("duplicate reserved twice: want %d, got %d", before-3, got)
	}
}

func TestPoisonMessageGoesToDLQ(t *testing.T) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, _ := nc.JetStream()

	count := func() uint64 {
		info, err := js.StreamInfo("DLQ")
		if err != nil {
			t.Fatal(err)
		}
		return info.State.Msgs
	}
	before := count()
	if _, err := js.Publish(events.Subject(events.TypeOrderCancelled), []byte("not json")); err != nil {
		t.Fatal(err)
	}
	eventually(t, "poison message in DLQ", 10*time.Second, func() bool { return count() > before })
}

// Compensation after a successful payment: cancelling the order must release
// the stock AND refund the charge.
func TestCancelledOrderIsRefundedAndStockReleased(t *testing.T) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, _ := nc.JetStream()
	sub, err := js.SubscribeSync(events.Subject(events.TypePaymentRefunded), nats.DeliverNew())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	before := stock(t)["sku-1"]
	id, _ := createOrder(t, "", item{"sku": "sku-1", "quantity": 4, "unit_price_cents": 500})
	waitStatus(t, id, "CONFIRMED")
	if got := stock(t)["sku-1"]; got != before-4 {
		t.Fatalf("stock after order: want %d, got %d", before-4, got)
	}

	cancel, _ := events.New(events.TypeOrderCancelled, events.OrderCancelled{OrderID: id, Reason: "e2e"})
	body, _ := json.Marshal(cancel)
	if _, err := js.Publish(events.Subject(events.TypeOrderCancelled), body); err != nil {
		t.Fatal(err)
	}

	eventually(t, "stock released", 15*time.Second, func() bool { return stock(t)["sku-1"] == before })
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		msg, err := sub.NextMsg(time.Second)
		if err != nil {
			continue
		}
		var env events.Envelope
		_ = json.Unmarshal(msg.Data, &env)
		if r, _ := events.Decode[events.PaymentRefunded](env); r.OrderID == id {
			return
		}
	}
	t.Fatal("no payment.refunded event for the cancelled order")
}

func TestGRPCAPI(t *testing.T) {
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c := ordersv1.NewOrdersServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &ordersv1.CreateOrderRequest{
		CustomerId:     "grpc",
		IdempotencyKey: uuid.NewString(),
		Items:          []*ordersv1.Item{{Sku: "sku-2", Quantity: 1, UnitPriceCents: 1000}},
	}
	first, err := c.CreateOrder(ctx, req)
	if err != nil || !first.Created {
		t.Fatalf("first create: %v %+v", err, first)
	}
	again, err := c.CreateOrder(ctx, req)
	if err != nil || again.Created || again.Id != first.Id {
		t.Fatalf("idempotent retry: %v %+v", err, again)
	}
	waitStatus(t, first.Id, "CONFIRMED")
	got, err := c.GetOrder(ctx, &ordersv1.GetOrderRequest{Id: first.Id})
	if err != nil || got.Status != "CONFIRMED" || got.TotalCents != 1000 || len(got.Items) != 1 {
		t.Fatalf("get: %v %+v", err, got)
	}

	if _, err := c.GetOrder(ctx, &ordersv1.GetOrderRequest{Id: uuid.NewString()}); status.Code(err) != codes.NotFound {
		t.Fatalf("want NotFound, got %v", err)
	}
	if _, err := c.CreateOrder(ctx, &ordersv1.CreateOrderRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument, got %v", err)
	}
}
