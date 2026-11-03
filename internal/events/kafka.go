package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// KafkaPublisher publishes events to a Kafka or Redpanda topic. The
// message key is the event's partition key (its account), and the writer
// uses a hash balancer, so all events for one account land in one
// partition, in publish order — the broker-side half of the per-account
// ordering guarantee. RequiredAcks is RequireAll: an event is only treated
// as published once every in-sync replica has it, so a broker failure
// after acknowledgement cannot silently lose an event the relay already
// marked published.
type KafkaPublisher struct {
	writer *kafka.Writer
}

// NewKafkaPublisher returns a publisher for topic on brokers (a list of
// host:port addresses, e.g. from KAFKA_BROKERS). It returns an error if
// no broker or no topic is given; it does not dial the broker here, so a
// broker that is down surfaces on the first Publish, where the relay's
// retry logic handles it.
func NewKafkaPublisher(brokers []string, topic string) (*KafkaPublisher, error) {
	if len(brokers) == 0 {
		return nil, errors.New("events: at least one Kafka broker is required")
	}
	if topic == "" {
		return nil, errors.New("events: Kafka topic is required")
	}
	return &KafkaPublisher{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
		},
	}, nil
}

// Publish serialises the event to JSON and writes one message, keyed by
// the event's partition key.
func (k *KafkaPublisher) Publish(ctx context.Context, event Event) error {
	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("events: marshal event %s: %w", event.ID, err)
	}
	msg := kafka.Message{
		Key:   []byte(event.PartitionKey()),
		Value: value,
		Headers: []kafka.Header{
			{Key: "event_id", Value: []byte(event.ID)},
			{Key: "event_type", Value: []byte(event.Type)},
		},
	}
	if err := k.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("events: publish %s: %w", event.ID, err)
	}
	return nil
}

// Close flushes and closes the underlying writer.
func (k *KafkaPublisher) Close() error {
	if k.writer == nil {
		return nil
	}
	return k.writer.Close()
}
