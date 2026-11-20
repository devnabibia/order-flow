package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ordersv1 "github.com/example/orderflow/gen/orders/v1"
	"github.com/example/orderflow/pkg/events"
)

// grpcServer adapts Service to the generated OrdersService interface.
type grpcServer struct {
	ordersv1.UnimplementedOrdersServiceServer
	svc *Service
}

func (g *grpcServer) CreateOrder(ctx context.Context, req *ordersv1.CreateOrderRequest) (*ordersv1.CreateOrderResponse, error) {
	items := make([]events.Item, 0, len(req.GetItems()))
	for _, it := range req.GetItems() {
		items = append(items, events.Item{SKU: it.GetSku(), Quantity: int(it.GetQuantity()), UnitPriceCents: it.GetUnitPriceCents()})
	}
	id, st, created, err := g.svc.Create(ctx, req.GetCustomerId(), items, req.GetIdempotencyKey())
	if err != nil {
		return nil, grpcErr(ctx, err)
	}
	return &ordersv1.CreateOrderResponse{Id: id, Status: st, Created: created}, nil
}

func (g *grpcServer) GetOrder(ctx context.Context, req *ordersv1.GetOrderRequest) (*ordersv1.Order, error) {
	o, err := g.svc.Get(ctx, req.GetId())
	if err != nil {
		return nil, grpcErr(ctx, err)
	}
	out := &ordersv1.Order{
		Id: o.ID, CustomerId: o.CustomerID, Status: o.Status, TotalCents: o.TotalCents,
		CreatedAt: o.CreatedAt.UTC().Format(time.RFC3339),
	}
	for _, it := range o.Items {
		out.Items = append(out.Items, &ordersv1.Item{Sku: it.SKU, Quantity: int32(it.Quantity), UnitPriceCents: it.UnitPriceCents})
	}
	return out, nil
}

func grpcErr(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrNotFound):
		return status.Error(codes.NotFound, "order not found")
	default:
		slog.ErrorContext(ctx, "grpc request failed", "err", err)
		return status.Error(codes.Internal, "internal error")
	}
}
