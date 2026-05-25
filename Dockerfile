FROM golang:alpine AS build

WORKDIR /app

RUN apk add --no-cache ca-certificates git

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/cache-server ./cmd/server

FROM alpine:latest

COPY --from=build --chmod=0755 /out/cache-server /cache-server

RUN apk add --no-cache ca-certificates \
    && addgroup -S cache \
    && adduser -S -G cache cache

USER cache:cache
EXPOSE 3000
ENTRYPOINT ["/cache-server"]
