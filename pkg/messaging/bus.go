// Package messaging wraps NATS JetStream with OpenTelemetry trace propagation.
package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/example/orderflow/pkg/events"
	"github.com/example/orderflow/pkg/metrics"
)

var tracer = otel.Tracer("orderflow/messaging")

const (
	StreamName    = "EVENTS"
	DLQStreamName = "DLQ"
	maxDeliver    = 5
)

type Bus struct {
	nc *nats.Conn
	js nats.JetStreamContext
}

func Connect(url string) (*Bus, error) {
	nc, err := nats.Connect(url, nats.MaxReconnects(-1))
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, err
	}
	_, err = js.AddStream(&nats.StreamConfig{
		Name:       StreamName,
		Subjects:   []string{"events.>"},
		Duplicates: 2 * time.Minute, // broker-side dedup window for Nats-Msg-Id
	})
	if err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		nc.Close()
		return nil, err
	}
	// Dead-letter stream: messages that exhausted their retries land here for inspection/replay.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     DLQStreamName,
		Subjects: []string{"dlq.>"},
		MaxAge:   7 * 24 * time.Hour,
	})
	if err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		nc.Close()
		return nil, err
	}
	return &Bus{nc: nc, js: js}, nil
}

// Publish sends a message. The current span context in ctx is injected into
// the message headers so consumers continue the same trace. id is used as
// Nats-Msg-Id so the broker drops duplicate publishes from outbox retries.
func (b *Bus) Publish(ctx context.Context, id, subject string, body []byte) error {
	ctx, span := tracer.Start(ctx, "publish "+subject, trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	msg := nats.NewMsg(subject)
	msg.Data = body
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(msg.Header))
	msg.Header.Set(nats.MsgIdHdr, id)

	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := b.js.PublishMsg(msg, nats.Context(pctx)); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

// Consume subscribes with a durable queue group (so replicas share work).
// A failing handler is retried with a delay; after maxDeliver attempts the
// message is moved to the DLQ stream instead of blocking the consumer.
func (b *Bus) Consume(durable, subject string, h func(ctx context.Context, env events.Envelope) error) error {
	_, err := b.js.QueueSubscribe(subject, durable, func(m *nats.Msg) {
		ctx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.HeaderCarrier(m.Header))
		ctx, span := tracer.Start(ctx, "consume "+m.Subject, trace.WithSpanKind(trace.SpanKindConsumer))
		defer span.End()

		var env events.Envelope
		if err := json.Unmarshal(m.Data, &env); err != nil {
			span.RecordError(err)
			slog.ErrorContext(ctx, "malformed message, sending to DLQ", "subject", m.Subject, "err", err)
			metrics.EventsProcessed.WithLabelValues(m.Subject, "malformed").Inc()
			b.deadLetter(ctx, m, err) // retrying can never fix a poison message
			return
		}

		if err := h(ctx, env); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			if md, merr := m.Metadata(); merr == nil && md.NumDelivered >= maxDeliver {
				slog.ErrorContext(ctx, "retries exhausted, sending to DLQ", "event", env.Type, "id", env.ID, "err", err)
				metrics.EventsProcessed.WithLabelValues(env.Type, "dlq").Inc()
				b.deadLetter(ctx, m, err)
				return
			}
			slog.WarnContext(ctx, "handler failed, will retry", "event", env.Type, "id", env.ID, "err", err)
			metrics.EventsProcessed.WithLabelValues(env.Type, "retry").Inc()
			_ = m.NakWithDelay(2 * time.Second)
			return
		}
		metrics.EventsProcessed.WithLabelValues(env.Type, "ok").Inc()
		_ = m.Ack()
	}, nats.Durable(durable), nats.ManualAck(), nats.AckExplicit(),
		nats.MaxDeliver(maxDeliver), nats.AckWait(30*time.Second))
	return err
}

// deadLetter copies the message to dlq.<subject> (with the failure reason) and
// terminates it. If the copy itself fails, the message is nacked so nothing is lost.
func (b *Bus) deadLetter(ctx context.Context, m *nats.Msg, cause error) {
	dl := nats.NewMsg("dlq." + m.Subject)
	dl.Data = m.Data
	dl.Header.Set("X-Error", cause.Error())
	dl.Header.Set("X-Original-Subject", m.Subject)
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := b.js.PublishMsg(dl, nats.Context(pctx)); err != nil {
		slog.ErrorContext(ctx, "DLQ publish failed, nacking", "err", err)
		_ = m.NakWithDelay(5 * time.Second)
		return
	}
	_ = m.Term()
}

// Close drains in-flight messages and closes the connection.
func (b *Bus) Close() { _ = b.nc.Drain() }
