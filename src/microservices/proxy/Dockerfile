FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o proxy-service .

FROM alpine:3.19
RUN addgroup -S app && adduser -S app -G app
WORKDIR /app
COPY --from=builder /app/proxy-service /app/proxy-service
USER app
EXPOSE 8000
ENTRYPOINT ["/app/proxy-service"]
