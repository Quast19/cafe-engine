package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"cafe-engine/internal/kafka"
	"cafe-engine/internal/model"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/google/uuid"
)

// Broadcaster manages real-time SSE client connections
type Broadcaster struct {
	clients    map[chan string]bool
	newClients chan chan string
	defClients chan chan string
	messages   chan string
	mu         sync.Mutex
}

var broadcaster = &Broadcaster{
	clients:    make(map[chan string]bool),
	newClients: make(chan chan string),
	defClients: make(chan chan string),
	messages:   make(chan string),
}

func (b *Broadcaster) Listen() {
	for {
		select {
		case s := <-b.newClients:
			b.mu.Lock()
			b.clients[s] = true
			b.mu.Unlock()
		case s := <-b.defClients:
			b.mu.Lock()
			delete(b.clients, s)
			close(s)
			b.mu.Unlock()
		case msg := <-b.messages:
			b.mu.Lock()
			for clientChan := range b.clients {
				clientChan <- msg
			}
			b.mu.Unlock()
		}
	}
}

// CORS Middleware helper
func enableCORS(w http.ResponseWriter, r *http.Request) bool {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

	// If browser is sending a preflight OPTIONS request, respond 200 OK immediately
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return true
	}
	return false
}

func main() {
	go broadcaster.Listen()

	producer := kafka.NewProducer("localhost:9092", "order-created")
	// Listen to order status updates from Kitchen and stream to SSE clients
	go func() {
		statusReader := kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: []string{"127.0.0.1:9092"},
			Topic:   "order-status-updated",
			GroupID: "api-sse-group",
		})

		log.Println("📡 API Server listening for kitchen status updates...")

		for {
			m, err := statusReader.ReadMessage(context.Background())
			if err != nil {
				log.Printf("Error reading status update: %v", err)
				break
			}
			// Broadcast updated status (COOKING / COMPLETED) to SSE React clients
			broadcaster.messages <- string(m.Value)
		}
	}()
	// POST /orders Handler
	http.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
		// Handle CORS Preflight
		if enableCORS(w, r) {
			return
		}

		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			Items []model.ItemType `json:"items"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request payload", http.StatusBadRequest)
			return
		}

		order := model.Order{
			ID:        uuid.New().String(),
			Items:     req.Items,
			Status:    "CREATED",
			CreatedAt: time.Now(),
		}

		if err := producer.PublishOrder(context.Background(), order); err != nil {
			log.Printf("Failed to publish order: %v", err)
			http.Error(w, "Failed to process order", http.StatusInternalServerError)
			return
		}

		// Broadcast order via SSE
		orderJSON, _ := json.Marshal(order)
		broadcaster.messages <- string(orderJSON)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(order)
	})

	// GET /orders/stream SSE Handler
	http.HandleFunc("/orders/stream", func(w http.ResponseWriter, r *http.Request) {
		if enableCORS(w, r) {
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		messageChan := make(chan string)
		broadcaster.newClients <- messageChan

		defer func() {
			broadcaster.defClients <- messageChan
		}()

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
			return
		}

		notify := r.Context().Done()
		for {
			select {
			case <-notify:
				return
			case msg := <-messageChan:
				fmt.Fprintf(w, "data: %s\n\n", msg)
				flusher.Flush()
			}
		}
	})

	log.Println("🚀 API Server with CORS & SSE running on http://localhost:8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
