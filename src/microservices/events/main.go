package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	movieTopic   = "movie-events"
	userTopic    = "user-events"
	paymentTopic = "payment-events"
)

var managedTopics = []string{movieTopic, userTopic, paymentTopic}

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

type EventBus struct {
	brokers []string
	writers map[string]*kafka.Writer

	mu     sync.RWMutex
	events []Event
	seen   map[string]struct{}
}

func NewEventBus(brokers []string) *EventBus {
	writers := make(map[string]*kafka.Writer, len(managedTopics))
	for _, topic := range managedTopics {
		writers[topic] = &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
			BatchSize:    1,
			BatchTimeout: 10 * time.Millisecond,
			WriteTimeout: 5 * time.Second,
			ReadTimeout:  5 * time.Second,
		}
	}
	return &EventBus{
		brokers: brokers,
		writers: writers,
		events:  make([]Event, 0, 256),
		seen:    make(map[string]struct{}, 256),
	}
}

func (b *EventBus) Start(ctx context.Context) {
	go b.ensureKafkaTopicsUntilReady(ctx)
	for _, topic := range managedTopics {
		go b.consumeTopic(ctx, topic)
	}
}

func (b *EventBus) Close() {
	for _, writer := range b.writers {
		if err := writer.Close(); err != nil {
			log.Printf("failed to close Kafka writer: %v", err)
		}
	}
}

func (b *EventBus) Publish(ctx context.Context, event Event) error {
	writer, ok := b.writers[event.Topic]
	if !ok {
		return fmt.Errorf("unsupported topic %q", event.Topic)
	}

	messageValue, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	message := kafka.Message{
		Key:   []byte(event.ID),
		Value: messageValue,
		Time:  event.CreatedAt,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(event.Type)},
		},
	}

	writeCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	var lastErr error
	for attempt := 1; attempt <= 5; attempt++ {
		if err := writer.WriteMessages(writeCtx, message); err != nil {
			lastErr = err
			log.Printf("Kafka publish attempt=%d topic=%s event_id=%s failed: %v", attempt, event.Topic, event.ID, err)
			time.Sleep(time.Duration(attempt) * 300 * time.Millisecond)
			continue
		}

		// Store immediately so GET /api/events is deterministic for tests.
		// The consumer also stores the same event after reading it from Kafka; store() deduplicates by ID.
		b.store(event)
		log.Printf("PRODUCED topic=%s event_id=%s type=%s payload=%s", event.Topic, event.ID, event.Type, toJSON(event.Payload))
		return nil
	}

	if lastErr == nil {
		lastErr = errors.New("unknown Kafka publish error")
	}
	return fmt.Errorf("publish event to Kafka topic %s: %w", event.Topic, lastErr)
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
	if _, exists := b.seen[event.ID]; exists {
		return
	}
	b.seen[event.ID] = struct{}{}
	b.events = append(b.events, event)
	if len(b.events) > 1000 {
		oldest := b.events[0]
		delete(b.seen, oldest.ID)
		b.events = b.events[1:]
	}
}

func (b *EventBus) consumeTopic(ctx context.Context, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        b.brokers,
		Topic:          topic,
		GroupID:        "cinemaabyss-events-service",
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
		MaxWait:        500 * time.Millisecond,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("failed to close Kafka reader for topic=%s: %v", topic, err)
		}
	}()

	log.Printf("Kafka consumer started topic=%s brokers=%s", topic, strings.Join(b.brokers, ","))
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			log.Printf("Kafka consumer topic=%s fetch failed: %v", topic, err)
			time.Sleep(2 * time.Second)
			continue
		}

		var event Event
		if err := json.Unmarshal(message.Value, &event); err != nil {
			log.Printf("Kafka consumer topic=%s offset=%d invalid event: %v", topic, message.Offset, err)
			_ = reader.CommitMessages(ctx, message)
			continue
		}

		b.store(event)
		log.Printf("CONSUMED topic=%s partition=%d offset=%d event_id=%s type=%s payload=%s", topic, message.Partition, message.Offset, event.ID, event.Type, toJSON(event.Payload))
		if err := reader.CommitMessages(ctx, message); err != nil {
			log.Printf("Kafka consumer topic=%s offset=%d commit failed: %v", topic, message.Offset, err)
		}
	}
}

func (b *EventBus) ensureKafkaTopicsUntilReady(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		if err := b.ensureKafkaTopics(ctx); err != nil {
			log.Printf("Kafka topics are not ready yet: %v", err)
		} else {
			log.Printf("Kafka is reachable; topics ready: %s", strings.Join(managedTopics, ","))
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (b *EventBus) ensureKafkaTopics(ctx context.Context) error {
	if len(b.brokers) == 0 {
		return errors.New("KAFKA_BROKERS is empty")
	}

	broker := b.brokers[0]
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		return fmt.Errorf("connect broker %s: %w", broker, err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("read Kafka controller: %w", err)
	}

	controllerAddress := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	controllerConn, err := kafka.DialContext(ctx, "tcp", controllerAddress)
	if err != nil {
		return fmt.Errorf("connect Kafka controller %s: %w", controllerAddress, err)
	}
	defer controllerConn.Close()

	topicConfigs := make([]kafka.TopicConfig, 0, len(managedTopics))
	for _, topic := range managedTopics {
		topicConfigs = append(topicConfigs, kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		})
	}
	if err := controllerConn.CreateTopics(topicConfigs...); err != nil && !strings.Contains(strings.ToLower(err.Error()), "already exists") {
		return fmt.Errorf("create Kafka topics: %w", err)
	}
	return nil
}

func main() {
	port := env("PORT", "8082")
	brokers := splitCSV(env("KAFKA_BROKERS", "kafka:9092"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bus := NewEventBus(brokers)
	defer bus.Close()
	bus.Start(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/events/health", healthHandler)
	mux.HandleFunc("/api/events/movie", createEventHandler(bus, "movie", movieTopic))
	mux.HandleFunc("/api/events/user", createEventHandler(bus, "user", userTopic))
	mux.HandleFunc("/api/events/payment", createEventHandler(bus, "payment", paymentTopic))
	mux.HandleFunc("/api/events", listEventsHandler(bus))

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           requestLogMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("events-service started on port %s; kafka brokers=%s", port, strings.Join(brokers, ","))
	log.Fatal(server.ListenAndServe())
}

func createEventHandler(bus *EventBus, eventType, topic string) http.HandlerFunc {
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

		now := time.Now().UTC()
		event := Event{
			ID:        fmt.Sprintf("%s-%d", eventType, now.UnixNano()),
			Type:      eventType,
			Topic:     topic,
			Payload:   payload,
			CreatedAt: now,
		}
		if err := bus.Publish(r.Context(), event); err != nil {
			log.Printf("failed to publish event type=%s topic=%s: %v", eventType, topic, err)
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
