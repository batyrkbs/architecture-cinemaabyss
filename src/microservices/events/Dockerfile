FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o events-service .

FROM alpine:3.19
RUN addgroup -S app && adduser -S app -G app
WORKDIR /app
COPY --from=builder /app/events-service /app/events-service
USER app
EXPOSE 8082
ENTRYPOINT ["/app/events-service"]
