# orderflow

A complete event-driven order system in Go: **orders**, **inventory** and **payments** services
that coordinate through NATS JetStream with a choreographed saga, plus a gRPC/HTTP API, a web
dashboard, tracing, metrics, and an end-to-end test suite.

```
            ┌────────────── dashboard (/) · HTTP :8080 · gRPC :9000 ──────────────┐
            ▼                                                                      │
        orders ──order.created──▶ NATS JetStream ──▶ inventory ──inventory.reserved──▶ payments
          ▲  ▲                         (EVENTS, DLQ)      │ inventory.rejected             │
          │  └──────── payment.succeeded / payment.failed ◀───────────────────────────────┘
          └── order.cancelled ──▶ inventory (release stock) + payments (refund)
```

## Quick start

```bash
go mod tidy                                            # optional locally; Docker does it itself
docker compose -f deploy/docker-compose.yml up --build   # or: make up
```

| What | Where |
|---|---|
| Dashboard (place orders, watch status) | http://localhost:8080 |
| Jaeger (distributed traces) | http://localhost:16686 |
| Prometheus | http://localhost:9091 |
| Stock levels | http://localhost:8081/stock |
| gRPC (reflection enabled) | `localhost:9000` |

Dashboard presets trigger each saga path: **Happy path** (CONFIRMED), **Out of stock** (CANCELLED
by inventory), **Payment declined** (stock reserved, then released, CANCELLED).

### API

```bash
# HTTP
curl -s localhost:8080/orders -H 'Idempotency-Key: demo-1' -d '{
  "customer_id":"c1","items":[{"sku":"sku-1","quantity":2,"unit_price_cents":1500}]}'
curl -s localhost:8080/orders/<id>
curl -s localhost:8080/orders            # latest 25

# gRPC
grpcurl -plaintext -d '{"customer_id":"c1","items":[{"sku":"sku-1","quantity":1,"unit_price_cents":900}]}' \
  localhost:9000 orders.v1.OrdersService/CreateOrder
```

Seed stock: `sku-1` 100, `sku-2` 100, `sku-3` 5, `sku-4` 100. The fake payment provider declines
any order over $1000.

## The saga

1. **orders** stores the order as `PENDING` and the `order.created` event in one transaction.
2. **inventory** reserves stock (all-or-nothing via a savepoint) and emits `inventory.reserved`,
   or `inventory.rejected` if any SKU is short.
3. **payments** charges and emits `payment.succeeded` or `payment.failed`.
4. **orders** finalises: `CONFIRMED` on success; `CANCELLED` on rejection, failed payment, or a
   60-second timeout. Every cancellation emits `order.cancelled`.
5. **Compensation:** on `order.cancelled`, inventory releases the reservation and payments
   refunds a successful charge (`payment.refunded`).

Cancellation can overtake the events it cancels, so inventory and payments leave a *tombstone*
row when `order.cancelled` arrives first, and never reserve or charge for a dead order.

## Reliability patterns, and where they live

| Pattern | Location |
|---|---|
| Transactional outbox + relay (trace context saved with each row) | `pkg/outbox` |
| Idempotent consumers (inbox table, same tx as business writes) | `outbox.HandleOnce`, used by `boot.App.On` |
| Idempotent API (`Idempotency-Key`) | `Service.Create` in `services/orders/service.go` |
| Retries with delay, then **dead-letter queue** (`DLQ` stream, `dlq.>`) | `pkg/messaging` |
| Status-guarded transitions (late/duplicate/out-of-order events are no-ops) | `finalise`, saga handlers |
| Timeout reaper for stuck orders | `Service.reapStale` |
| Broker-side dedup (`Nats-Msg-Id` = event id) | `Bus.Publish` |
| Queue-group consumers (scale a service to N replicas safely) | `Bus.Consume` |

## Observability

- **Traces:** W3C trace context is injected on publish and extracted on consume, and saved in
  the outbox row, so one order is a single trace across HTTP/gRPC → outbox → NATS → all services.
  Every log line carries `trace_id`.
- **Metrics** (`:9090/metrics` per service, scraped by Prometheus): `events_processed_total{event,result}`
  (ok/retry/dlq/malformed), `outbox_pending`, `orders_finalised_total{status}`, `saga_duration_seconds`.

## Tests

```bash
make test    # unit tests
make up      # in another terminal, then:
make e2e     # 8 end-to-end tests against the running stack
```

The e2e suite covers: happy path, idempotency key (HTTP and gRPC), out-of-stock cancel, declined
payment releasing stock, **duplicate event delivery** (reserved exactly once), **poison message → DLQ**,
and **refund + stock release after cancellation**.

## Layout

```
services/orders      service.go (logic) · http.go · grpc.go · web/index.html (dashboard)
services/inventory   stock + reservations, GET /stock
services/payments    fake provider, refunds
pkg/{events,messaging,outbox,boot,telemetry,metrics}
proto/ + gen/        orders.proto and generated code (regenerate with `make proto`)
test/e2e             end-to-end tests (build tag: e2e)
deploy/              Dockerfile, docker-compose.yml, prometheus.yml
```

## Known limits (intentional, for a demo)

- Prices come from the client; a real system would look them up server-side.
- Schemas are applied at startup; use `golang-migrate` for real migrations.
- No authentication on the APIs.
- The timeout reaper (60 s) has no e2e test because it would make the suite slow.
- DLQ messages are stored but there is no replay tool yet.
