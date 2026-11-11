package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/didok362/cloud-platform/pkg/env"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	httpPort := env.Get("HTTP_PORT", "8082")
	natsURL := env.Get("NATS_URL", nats.DefaultURL)

	nc, err := nats.Connect(natsURL)
	if err != nil {
		logger.Error("nats_connect_failed", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		logger.Error("jetstream_init_failed", "error", err)
		os.Exit(1)
	}

	initCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// Гарантируем, что поток существует, даже если воркер стартовал раньше chat-engine
	_, err = js.CreateOrUpdateStream(initCtx, jetstream.StreamConfig{
		Name:     "CHAT_EVENTS",
		Subjects: []string{"chat.>"},
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		cancel()
		logger.Error("stream_init_failed", "error", err)
		os.Exit(1)
	}

	// Durable Consumer: NATS запоминает, какие сообщения уже обработаны
	cons, err := js.CreateOrUpdateConsumer(initCtx, "CHAT_EVENTS", jetstream.ConsumerConfig{
		Durable:       "notification-workers",
		FilterSubject: "chat.messages.created",
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	cancel()
	if err != nil {
		logger.Error("consumer_init_failed", "error", err)
		os.Exit(1)
	}

	consumeCtx, err := cons.Consume(func(msg jetstream.Msg) {
		var ev struct {
			ID     string `json:"id"`
			Sender string `json:"sender"`
			Text   string `json:"text"`
		}
		if err := json.Unmarshal(msg.Data(), &ev); err != nil {
			logger.Error("invalid_event_payload", "error", err)
			_ = msg.Term() // Битое сообщение не крутим в очереди бесконечно
			return
		}

		reqID := msg.Headers().Get("X-Request-Id")
		time.Sleep(150 * time.Millisecond) // Имитация отправки push/email уведомления
		logger.Info("notification_sent", "msg_id", ev.ID, "sender", ev.Sender, "request_id", reqID)
		_ = msg.Ack()
	})
	if err != nil {
		logger.Error("consume_failed", "error", err)
		os.Exit(1)
	}
	defer consumeCtx.Stop()

	var isReady atomic.Bool
	isReady.Store(true)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"alive"}`))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if !isReady.Load() || !nc.IsConnected() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"not_ready"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})

	srv := &http.Server{
		Addr:              ":" + httpPort,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("starting_worker", "service", "notification-worker", "port", httpPort)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http_probe_error", "error", err)
		}
	}()

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	<-sigCtx.Done()

	logger.Info("shutdown_signal_received")
	isReady.Store(false)
	consumeCtx.Stop()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
	_ = nc.Drain()
	logger.Info("graceful_shutdown_completed")
}
