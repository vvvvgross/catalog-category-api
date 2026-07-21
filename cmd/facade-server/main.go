package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/vvvvgross/catalog-category-api/internal/config"
)

func main() {
	log.SetOutput(os.Stdout)

	if err := config.ReadConfigYML("config.yml"); err != nil {
		log.Fatalf("Failed init configuration: %v", err)
	}
	cfg := config.GetConfigInstance()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Println("Facade server starting...")

	topics := []string{
		cfg.Kafka.Topics.Created,
		cfg.Kafka.Topics.Updated,
		cfg.Kafka.Topics.Removed,
	}

	var wg sync.WaitGroup

	for _, topic := range topics {
		wg.Add(1)

		go func(topic string) {
			defer wg.Done()
			runReader(ctx, cfg.Kafka.Brokers, cfg.Kafka.GroupID, topic)
		}(topic)
	}

	<-ctx.Done()
	wg.Wait()

	log.Println("Facade server stopped.")
}

func runReader(ctx context.Context, brokers []string, groupID string, topic string) {
	readerConfig := kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second, // Раз в секунду автоматически фиксируем прочитанные сообщения
	}

	reader := kafka.NewReader(readerConfig)

	defer func() {
		log.Printf("Closing Kafka reader for topic: %s", topic)
		if err := reader.Close(); err != nil {
			log.Printf("Failed to close reader: %v", err)
		}
	}()

	log.Printf("Listening to topic: %s", topic)

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("Context canceled, stopping reader loop.")
				break
			}

			log.Printf("Error while reading message: %v", err)
			continue
		}

		log.Printf(
			"Received message: Topic: %s | Key: %s | Value: %s | Partition: %d | Offset: %d",
			msg.Topic,
			string(msg.Key),
			string(msg.Value),
			msg.Partition,
			msg.Offset,
		)
	}
}
