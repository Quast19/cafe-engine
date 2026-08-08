package main

import (
	"cafe-engine/internal/kitchen"
	"cafe-engine/internal/model"
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	OrderBuffer := 100
	ChefNumber := 3
	// Initialize the Dispatcher with a buffer of 100 orders and 3 Chef Workers
	dispatcher := kitchen.NewDispatcher(OrderBuffer, ChefNumber)
	dispatcher.Start()

	// Configure Kafka Reader (Consumer Group)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "order-created",
		GroupID: "kitchen-group", // Consumer group ensures distributed reading
	})

	log.Println("🍳 Kitchen Service started! Listening for orders on Kafka...")

	// Infinite loop to continuously pull messages from Kafka
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			break
		}

		var order model.Order
		if err := json.Unmarshal(m.Value, &order); err == nil {
			// Push order into the Go channel for Chef workers to pick up
			dispatcher.OrderChan <- order
		} else {
			log.Printf("Failed to unmarshal order: %v", err)
		}
	}
}
