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

	chatv1 "github.com/didok362/cloud-platform/gen/chat/v1"
	"github.com/didok362/cloud-platform/pkg/env"
	"github.com/didok362/cloud-platform/pkg/httpx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	port := env.Get("PORT", "8080")
	chatAddr := env.Get("CHAT_ENGINE_ADDR", "localhost:50051")
	rateRPS := float64(env.GetInt("RATE_LIMIT_RPS", 5))
	rateBurst := env.GetInt("RATE_LIMIT_BURST", 10)
	shutdownTimeout := time.Duration(env.GetInt("SHUTDOWN_TIMEOUT_SEC", 10)) * time.Second

	// Открываем долгоживущий HTTP/2 gRPC-канал к chat-engine (TLS позже обеспечит Istio)
	conn, err := grpc.NewClient(chatAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("grpc_client_init_failed", "error", err)
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()
	chatClient := chatv1.NewChatServiceClient(conn)

	var isReady atomic.Bool
	isReady.Store(true)

	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"alive"}`))
	})

	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !isReady.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"shutting_down"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})

	mux.HandleFunc("GET /api/v1/auth/verify", func(w http.ResponseWriter, r *http.Request) {
		if delay := r.URL.Query().Get("delay"); delay != "" {
			if d, err := time.ParseDuration(delay); err == nil && d <= 15*time.Second {
				select {
				case <-time.After(d):
				case <-r.Context().Done():
					return
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"service": "auth-gateway",
			"status":  "authenticated",
		})
	})

	// Входная точка: HTTP JSON -> вызов chat-engine по gRPC с жёстким таймаутом 3s
	mux.HandleFunc("POST /api/v1/messages", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Sender string `json:"sender"`
			Text   string `json:"text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil || input.Sender == "" || input.Text == "" {
			http.Error(w, "invalid payload: sender and text are required", http.StatusBadRequest)
			return
		}

		reqID := w.Header().Get("X-Request-Id")
		callCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		outCtx := metadata.NewOutgoingContext(callCtx, metadata.Pairs("x-request-id", reqID))
		resp, err := chatClient.SendMessage(outCtx, &chatv1.SendMessageRequest{
			Sender: input.Sender,
			Text:   input.Text,
		})
		if err != nil {
			logger.Error("grpc_send_failed", "error", err, "request_id", reqID)
			http.Error(w, "chat service unavailable", http.StatusBadGateway)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"id":     resp.GetId(),
			"status": resp.GetStatus(),
		})
	})

	rateLimiter := httpx.NewIPRateLimiter(rateRPS, rateBurst)
	handler := httpx.LoggingMiddleware(logger, rateLimiter.Middleware(logger, mux))

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("starting_service", "service", "auth-gateway", "port", port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	select {
	case err := <-errCh:
		logger.Error("server_fatal_error", "error", err)
		os.Exit(1)
	case <-sigCtx.Done():
		logger.Info("shutdown_signal_received")
	}

	isReady.Store(false)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful_shutdown_failed", "error", err)
		os.Exit(1)
	}
	logger.Info("graceful_shutdown_completed")
}
