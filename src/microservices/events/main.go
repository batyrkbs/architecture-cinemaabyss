package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type Event struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	Topic     string                 `json:"topic"`
	Payload   map[string]interface{} `json:"payload"`
	CreatedAt time.Time              `json:"created_at"`
}

type EventResponse struct {
	Status string `json:"status"`
	Event  Event  `json:"event"`
}

type Producer interface {
	Publish(ctx context.Context, event Event) error
}

type Consumer interface {
	Start(ctx context.Context)
}

type EventBus struct {
	brokers []string
	queue   chan Event
	mu      sync.RWMutex
	events  []Event
}

func NewEventBus(brokers []string) *EventBus {
	return &EventBus{
		brokers: brokers,
		queue:   make(chan Event, 1024),
		events:  make([]Event, 0, 256),
	}
}

func (b *EventBus) Start(ctx context.Context) {
	go b.checkKafkaAvailability(ctx)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-b.queue:
				b.store(event)
				log.Printf("CONSUMED topic=%s event_id=%s type=%s payload=%s", event.Topic, event.ID, event.Type, toJSON(event.Payload))
			}
		}
	}()
}

func (b *EventBus) Publish(ctx context.Context, event Event) error {
	select {
	case b.queue <- event:
		log.Printf("PRODUCED topic=%s event_id=%s type=%s payload=%s", event.Topic, event.ID, event.Type, toJSON(event.Payload))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(2 * time.Second):
		return fmt.Errorf("event queue timeout")
	}
}

func (b *EventBus) List() []Event {
	b.mu.RLock()
	defer b.mu.RUnlock()
	result := make([]Event, len(b.events))
	copy(result, b.events)
	return result
}

func (b *EventBus) store(event Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, event)
	if len(b.events) > 1000 {
		b.events = b.events[len(b.events)-1000:]
	}
}

func (b *EventBus) checkKafkaAvailability(ctx context.Context) {
	if len(b.brokers) == 0 {
		log.Println("KAFKA_BROKERS is empty; events MVP uses local queue")
		return
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		for _, broker := range b.brokers {
			conn, err := net.DialTimeout("tcp", broker, 2*time.Second)
			if err != nil {
				log.Printf("Kafka broker %s is not reachable yet: %v", broker, err)
				continue
			}
			_ = conn.Close()
			log.Printf("Kafka broker %s is reachable; topics expected: movie-events,user-events,payment-events", broker)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func main() {
	port := env("PORT", "8082")
	brokers := splitCSV(env("KAFKA_BROKERS", "kafka:9092"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bus := NewEventBus(brokers)
	bus.Start(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/events/health", healthHandler)
	mux.HandleFunc("/api/events/movie", createEventHandler(bus, "movie", "movie-events"))
	mux.HandleFunc("/api/events/user", createEventHandler(bus, "user", "user-events"))
	mux.HandleFunc("/api/events/payment", createEventHandler(bus, "payment", "payment-events"))
	mux.HandleFunc("/api/events", listEventsHandler(bus))

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           requestLogMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("events-service started on port %s; kafka brokers=%s", port, strings.Join(brokers, ","))
	log.Fatal(server.ListenAndServe())
}

func createEventHandler(bus Producer, eventType, topic string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()
		var payload map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		event := Event{
			ID:        fmt.Sprintf("%s-%d", eventType, time.Now().UnixNano()),
			Type:      eventType,
			Topic:     topic,
			Payload:   payload,
			CreatedAt: time.Now().UTC(),
		}
		if err := bus.Publish(r.Context(), event); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(EventResponse{Status: "success", Event: event})
	}
}

func listEventsHandler(bus *EventBus) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(bus.List())
	}
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func requestLogMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s duration=%s", r.Method, r.URL.RequestURI(), time.Since(start).String())
	})
}

func toJSON(v interface{}) string {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(data)
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}
