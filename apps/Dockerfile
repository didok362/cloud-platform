# syntax=docker/dockerfile:1

# --- Этап 1: Сборка (тяжёлый компилятор остаётся тут) ---
FROM golang:1.26-alpine AS builder
WORKDIR /src

# Сначала зависимости — ради быстрого кэша Docker
COPY go.mod go.sum ./
RUN go mod download

# Затем исходный код
COPY . .

ARG SERVICE
RUN test -n "$SERVICE" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /out/app ./${SERVICE}

# --- Этап 2: Финальный пустой образ (Runtime) ---
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=builder /out/app /app

USER nonroot:nonroot

ENTRYPOINT ["/app"]