package kafka

import (
	"context"
	"fmt"
	"strconv"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/vvvvgross/catalog-category-api/internal/config"
	"github.com/vvvvgross/catalog-category-api/internal/model"
)

type Producer struct {
	writer *kafkago.Writer
	topics config.KafkaTopics
}

func NewProducer(brokers []string, topics config.KafkaTopics) *Producer {
	return &Producer{
		writer: &kafkago.Writer{
			Addr:     kafkago.TCP(brokers...),
			Balancer: &kafkago.LeastBytes{},
		},
		topics: topics,
	}
}

func (p *Producer) Send(ctx context.Context, event model.CategoryEvent) error {
	topic, err := p.topicByEventType(event.Type)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(ctx, kafkago.Message{
		Topic: topic,
		Key:   []byte(strconv.FormatUint(event.CategoryID, 10)),
		Value: event.Payload,
		Headers: []kafkago.Header{
			{
				Key:   "event_id",
				Value: []byte(strconv.FormatUint(event.ID, 10)),
			},
			{
				Key:   "event_type",
				Value: []byte(event.Type),
			},
		},
	})
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

func (p *Producer) topicByEventType(eventType string) (string, error) {
	switch eventType {
	case model.CategoryEventTypeCreated:
		return p.topics.Created, nil
	case model.CategoryEventTypeUpdated:
		return p.topics.Updated, nil
	case model.CategoryEventTypeRemoved:
		return p.topics.Removed, nil
	default:
		return "", fmt.Errorf("unknown category event type: %s", eventType)
	}
}
