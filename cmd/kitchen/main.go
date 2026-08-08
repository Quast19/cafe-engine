package main

import (
	"context"
	"encoding/json"
	"log"

	"cafe-engine/internal/kitchen"
	"cafe-engine/internal/model"

	"github.com/segmentio/kafka-go"
)

// Helper function to publish status updates back to Kafka
func publishStatusUpdate(writer *kafka.Writer, order model.Order, status string) {
	order.Status = status
	bytes, err := json.Marshal(order)
	if err != nil {
		log.Printf("Failed to marshal status update: %v", err)
		return
	}

	err = writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(order.ID),
		Value: bytes,
	})
	if err != nil {
		log.Printf("Failed to publish status update to Kafka: %v", err)
	} else {
		log.Printf("📢 [KAFKA EVENT] Order %s status updated to '%s'", order.ID, status)
	}
}

func main() {
	// Status Writer for Kafka
	statusWriter := &kafka.Writer{
		Addr:     kafka.TCP("127.0.0.1:9092"),
		Topic:    "order-status-updated",
		Balancer: &kafka.LeastBytes{},
	}

	// Pass Kafka status updater into Dispatcher
	dispatcher := kitchen.NewDispatcher(100, 3, func(o model.Order, status string) {
		publishStatusUpdate(statusWriter, o, status)
	})
	dispatcher.Start()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"127.0.0.1:9092"},
		Topic:   "order-created",
		GroupID: "kitchen-group",
	})

	log.Println("🍳 Kitchen Service running...")

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			break
		}

		var order model.Order
		if err := json.Unmarshal(m.Value, &order); err == nil {
			dispatcher.OrderChan <- order
		}
	}
}
