package events

import "testing"

func TestEnvelopeRoundTrip(t *testing.T) {
	in := OrderCreated{
		OrderID: "o1", CustomerID: "c1", TotalCents: 3000,
		Items: []Item{{SKU: "sku-1", Quantity: 2, UnitPriceCents: 1500}},
	}
	env, err := New(TypeOrderCreated, in)
	if err != nil {
		t.Fatal(err)
	}
	if env.ID == "" || env.Version != 1 || env.Type != TypeOrderCreated {
		t.Fatalf("bad envelope: %+v", env)
	}
	out, err := Decode[OrderCreated](env)
	if err != nil {
		t.Fatal(err)
	}
	if out.OrderID != in.OrderID || len(out.Items) != 1 || out.Items[0].SKU != "sku-1" {
		t.Fatalf("round trip mismatch: %+v", out)
	}
}

func TestEnvelopeIDsAreUnique(t *testing.T) {
	a, _ := New(TypeOrderCreated, struct{}{})
	b, _ := New(TypeOrderCreated, struct{}{})
	if a.ID == b.ID {
		t.Fatal("two envelopes share an ID; idempotency would break")
	}
}

func TestSubject(t *testing.T) {
	if got := Subject(TypePaymentFailed); got != "events.payment.failed" {
		t.Fatalf("got %q", got)
	}
}
