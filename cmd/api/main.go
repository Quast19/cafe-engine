package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"cafe-engine/internal/kafka"
	"cafe-engine/internal/model"

	"github.com/google/uuid"
)

func main() {
	// 1. Initialize Kafka Producer connecting to Docker Kafka
	producer := kafka.NewProducer("localhost:9092", "order-created")

	// 2. Define the HTTP endpoint handler for POST /orders
	http.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Decode JSON payload from client
		var req struct {
			Items []model.ItemType `json:"items"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request payload", http.StatusBadRequest)
			return
		}

		// Construct Order struct
		order := model.Order{
			ID:        uuid.New().String(),
			Items:     req.Items,
			Status:    "CREATED",
			CreatedAt: time.Now(),
		}

		log.Printf("[API SERVER %s] Received HTTP POST for order %s", time.Now().Format("15:04:05.000"), order.ID)

		// Publish event to Kafka
		if err := producer.PublishOrder(context.Background(), order); err != nil {
			log.Printf("Failed to publish order: %v", err)
			http.Error(w, "Failed to process order", http.StatusInternalServerError)
			return
		}

		// Respond with 202 Accepted
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(order)
	})

	log.Println("API Server running on http://localhost:8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
