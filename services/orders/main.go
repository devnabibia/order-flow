package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	ordersv1 "github.com/example/orderflow/gen/orders/v1"
	"github.com/example/orderflow/pkg/boot"
	"github.com/example/orderflow/pkg/events"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	app, err := boot.New(ctx, "orders", schema)
	boot.Must(err)
	defer app.Close()

	svc := &Service{pool: app.Pool}

	// Saga reactions: these three events decide the final order state.
	boot.Must(app.On("orders-payment-succeeded", events.TypePaymentSucceeded, svc.onPaymentSucceeded))
	boot.Must(app.On("orders-payment-failed", events.TypePaymentFailed, svc.onPaymentFailed))
	boot.Must(app.On("orders-inventory-rejected", events.TypeInventoryRejected, svc.onInventoryRejected))
	go svc.reapStale(ctx)

	// gRPC API (with reflection, so `grpcurl -plaintext localhost:9000 list` works).
	lis, err := net.Listen("tcp", boot.Env("GRPC_ADDR", ":9000"))
	boot.Must(err)
	gs := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	ordersv1.RegisterOrdersServiceServer(gs, &grpcServer{svc: svc})
	reflection.Register(gs)
	go func() {
		slog.Info("orders gRPC listening", "addr", lis.Addr().String())
		if err := gs.Serve(lis); err != nil {
			slog.Error("grpc server failed", "err", err)
		}
	}()

	// HTTP API + dashboard.
	srv := &http.Server{
		Addr:              boot.Env("HTTP_ADDR", ":8080"),
		Handler:           otelhttp.NewHandler(svc.routes(), "orders-http"),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		gs.GracefulStop()
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()

	slog.Info("orders HTTP listening", "addr", srv.Addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		slog.Error("http server failed", "err", err)
	}
}
