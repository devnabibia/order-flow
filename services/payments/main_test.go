package main

import (
	"context"
	"errors"
	"testing"
)

func TestCharge(t *testing.T) {
	ctx := context.Background()
	if err := charge(ctx, "o1", 5_000); err != nil {
		t.Fatalf("small charge should succeed: %v", err)
	}
	if err := charge(ctx, "o2", declineAboveCents+1); !errors.Is(err, errDeclined) {
		t.Fatalf("large charge should be declined, got %v", err)
	}
}

func TestChargeRespectsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := charge(ctx, "o3", 100); err == nil {
		t.Fatal("expected context error")
	}
}
