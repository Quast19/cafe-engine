package kafka

import (
	"cafe-engine/internal/model"
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// Producer handles publishing messages to a Kafka topic
type Producer struct {
	writer *kafka.Writer
}

// NewProducer creates and returns a configured Kafka Producer pointer
func NewProducer(broker string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(broker),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

// PublishOrder converts an Order struct to JSON and writes it to Kafka
func (p *Producer) PublishOrder(ctx context.Context, order model.Order) error {
	bytes, err := json.Marshal(order)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(order.ID), // Order ID used as partition key
		Value: bytes,
	})

	// 2. Log confirmation AFTER Kafka responds
	log.Printf("[KAFKA PRODUCER %s] Successfully published order %s to topic '%s'",
		time.Now().Format("15:04:05.000"), order.ID, p.writer.Topic)

	return nil
}
