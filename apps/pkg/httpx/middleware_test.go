package httpx

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestIPRateLimiter_BurstAnd429(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rl := NewIPRateLimiter(0.01, 2)
	handler := rl.Middleware(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for i := 1; i <= 2; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/messages", nil)
		req.RemoteAddr = "10.0.0.1:1234"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: expected 200, got %d", i, rec.Code)
		}
	}

	// 3-й запрос подряд с того же IP должен упереться в лимит
	reqBlocked := httptest.NewRequest(http.MethodPost, "/api/v1/messages", nil)
	reqBlocked.RemoteAddr = "10.0.0.1:1234"
	recBlocked := httptest.NewRecorder()
	handler.ServeHTTP(recBlocked, reqBlocked)
	if recBlocked.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", recBlocked.Code)
	}
	if recBlocked.Header().Get("Retry-After") != "1" {
		t.Fatalf("expected Retry-After: 1, got %q", recBlocked.Header().Get("Retry-After"))
	}

	// Запрос с другого IP должен пройти без проблем
	reqOtherIP := httptest.NewRequest(http.MethodPost, "/api/v1/messages", nil)
	reqOtherIP.RemoteAddr = "10.0.0.2:5555"
	recOtherIP := httptest.NewRecorder()
	handler.ServeHTTP(recOtherIP, reqOtherIP)
	if recOtherIP.Code != http.StatusOK {
		t.Fatalf("other IP: expected 200, got %d", recOtherIP.Code)
	}
}

func TestIPRateLimiter_BypassHealthProbes(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rl := NewIPRateLimiter(0.01, 1)
	handler := rl.Middleware(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Исчерпываем лимит с одного IP
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/messages", nil)
		req.RemoteAddr = "10.0.0.1:1234"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}

	// Пробы Kubernetes с того же IP всё равно должны проходить
	for _, path := range []string{"/healthz", "/readyz"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.RemoteAddr = "10.0.0.1:1234"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s expected 200 even when rate-limited, got %d", path, rec.Code)
		}
	}
}

func TestLoggingMiddleware_SetsRequestID(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := LoggingMiddleware(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))

	// Без заголовка — генерируется автоматически
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Header().Get("X-Request-Id") == "" {
		t.Fatal("expected generated X-Request-Id header")
	}

	// С уже заданным заголовком — сохраняется исходный
	reqWithID := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	reqWithID.Header.Set("X-Request-Id", "trace-123")
	recWithID := httptest.NewRecorder()
	handler.ServeHTTP(recWithID, reqWithID)
	if got := recWithID.Header().Get("X-Request-Id"); got != "trace-123" {
		t.Fatalf("expected X-Request-Id trace-123, got %q", got)
	}
}

func TestIPRateLimiter_Concurrent(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rl := NewIPRateLimiter(100, 10)
	handler := rl.Middleware(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/api/v1/messages", nil)
			req.RemoteAddr = "10.0.0.5:9000"
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
		}()
	}
	wg.Wait()
}
