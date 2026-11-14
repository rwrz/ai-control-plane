# Build
FROM golang:1.26-alpine AS build

WORKDIR /src

# Dependencies first so a code-only change reuses the module layer.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Static binary: the runtime stage has no libc.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server

# Runtime
FROM alpine:3.20

# ca-certificates is required: the relay makes outbound TLS calls to the AI
# providers. Without it every upstream request fails certificate verification.
RUN apk add --no-cache ca-certificates \
    && adduser -D -u 10001 app

WORKDIR /app

COPY --from=build /out/server /app/server
# The pricing table is read at startup from PRICING_PATH (default: pricing.toml,
# resolved relative to WORKDIR). Ship it in the image so the service is
# self-contained; mount over it to change prices without a rebuild.
COPY pricing.toml /app/pricing.toml

USER app

EXPOSE 8080

ENTRYPOINT ["/app/server"]
