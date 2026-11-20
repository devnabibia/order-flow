// Package events defines the event envelope and payloads shared by all services.
package events

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

const (
	TypeOrderCreated      = "order.created"
	TypeOrderCancelled    = "order.cancelled"
	TypeInventoryReserved = "inventory.reserved"
	TypeInventoryRejected = "inventory.rejected"
	TypePaymentSucceeded  = "payment.succeeded"
	TypePaymentFailed     = "payment.failed"
	TypePaymentRefunded   = "payment.refunded"
)

// Subject maps an event type to its NATS subject.
func Subject(eventType string) string { return "events." + eventType }

// Envelope wraps every event. Version lets payloads evolve safely.
type Envelope struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Version    int             `json:"version"`
	OccurredAt time.Time       `json:"occurred_at"`
	Payload    json.RawMessage `json:"payload"`
}

func New(eventType string, payload any) (Envelope, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{
		ID:         uuid.NewString(),
		Type:       eventType,
		Version:    1,
		OccurredAt: time.Now().UTC(),
		Payload:    b,
	}, nil
}

func Decode[T any](env Envelope) (T, error) {
	var v T
	err := json.Unmarshal(env.Payload, &v)
	return v, err
}

type Item struct {
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int64  `json:"unit_price_cents"`
}

type OrderCreated struct {
	OrderID    string `json:"order_id"`
	CustomerID string `json:"customer_id"`
	Items      []Item `json:"items"`
	TotalCents int64  `json:"total_cents"`
}

type OrderCancelled struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

type InventoryReserved struct {
	OrderID    string `json:"order_id"`
	CustomerID string `json:"customer_id"`
	TotalCents int64  `json:"total_cents"`
}

type InventoryRejected struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

type PaymentSucceeded struct {
	OrderID string `json:"order_id"`
}

type PaymentFailed struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

type PaymentRefunded struct {
	OrderID string `json:"order_id"`
}
