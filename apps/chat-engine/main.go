package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	chatv1 "github.com/didok362/cloud-platform/gen/chat/v1"
	"github.com/didok362/cloud-platform/pkg/env"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

type chatServer struct {
	chatv1.UnimplementedChatServiceServer
	js     jetstream.JetStream
	logger *slog.Logger
}

func (s *chatServer) SendMessage(ctx context.Context, req *chatv1.SendMessageRequest) (*chatv1.SendMessageResponse, error) {
	if req.GetSender() == "" || req.GetText() == "" {
		return nil, status.Error(codes.InvalidArgument, "sender and text are required")
	}

	reqID := ""
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("x-request-id"); len(vals) > 0 {
			reqID = vals[0]
		}
	}

	msgID := fmt.Sprintf("msg-%d", time.Now().UnixNano())
	payload, err := json.Marshal(map[string]any{
		"id":         msgID,
		"sender":     req.GetSender(),
		"text":       req.GetText(),
		"created_at": time.Now().UTC(),
	})
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to serialize message")
	}

	_, err = s.js.PublishMsg(ctx, &nats.Msg{
		Subject: "chat.messages.created",
		Data:    payload,
		Header: nats.Header{
			"Nats-Msg-Id":  []string{msgID},
			"X-Request-Id": []string{reqID},
		},
	})
	if err != nil {
		s.logger.Error("nats_publish_failed", "error", err, "request_id", reqID)
		return nil, status.Error(codes.Unavailable, "queue unavailable")
	}

	s.logger.Info("message_queued", "msg_id", msgID, "sender", req.GetSender(), "request_id", reqID)
	return &chatv1.SendMessageResponse{
		Id:     msgID,
		Status: "queued",
	}, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	grpcPort := env.Get("GRPC_PORT", "50051")
	httpPort := env.Get("HTTP_PORT", "8081")
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
	_, err = js.CreateOrUpdateStream(initCtx, jetstream.StreamConfig{
		Name:     "CHAT_EVENTS",
		Subjects: []string{"chat.>"},
		Storage:  jetstream.FileStorage,
	})
	cancel()
	if err != nil {
		logger.Error("stream_create_failed", "error", err)
		os.Exit(1)
	}

	var isReady atomic.Bool
	isReady.Store(true)

	// Маленький HTTP-сервер чисто под Kubernetes-пробы
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"alive"}`))
	})
	healthMux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if !isReady.Load() || !nc.IsConnected() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"not_ready"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})

	httpSrv := &http.Server{
		Addr:              ":" + httpPort,
		Handler:           healthMux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		logger.Error("grpc_listen_failed", "error", err)
		os.Exit(1)
	}

	grpcSrv := grpc.NewServer()
	chatv1.RegisterChatServiceServer(grpcSrv, &chatServer{js: js, logger: logger})
	reflection.Register(grpcSrv)

	errCh := make(chan error, 2)
	go func() {
		logger.Info("starting_http_probes", "service", "chat-engine", "port", httpPort)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	go func() {
		logger.Info("starting_grpc_server", "service", "chat-engine", "port", grpcPort)
		if err := grpcSrv.Serve(lis); err != nil {
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
	grpcSrv.GracefulStop()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	_ = httpSrv.Shutdown(shutdownCtx)
	_ = nc.Drain()
	logger.Info("graceful_shutdown_completed")
}
