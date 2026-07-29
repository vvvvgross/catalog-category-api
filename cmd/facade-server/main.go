package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/segmentio/kafka-go"

	"github.com/vvvvgross/catalog-category-api/internal/config"
	"github.com/vvvvgross/catalog-category-api/internal/database"
	"github.com/vvvvgross/catalog-category-api/internal/facade"
)

type categoryPayload struct {
	CategoryID uint64 `json:"category_id"`
	Foo        string `json:"foo"`
}

type categoryTopics struct {
	Created string
	Updated string
	Removed string
}

func main() {
	log.SetOutput(os.Stdout)

	configPath := flag.String("config", "config.facade.local.yml", "path to config file")
	migration := flag.Bool("migration", true, "Defines the migration start option")
	flag.Parse()

	if err := config.ReadConfigYML(*configPath); err != nil {
		log.Fatalf("Failed init configuration: %v", err)
	}

	cfg := config.GetConfigInstance()

	log.Printf("Config path: %s", *configPath)
	log.Printf("Kafka brokers: %v", cfg.Kafka.Brokers)

	dsn := fmt.Sprintf("host=%v port=%v user=%v password=%v dbname=%v sslmode=%v",
		cfg.Database.Host,
		cfg.Database.Port,
		cfg.Database.User,
		cfg.Database.Password,
		cfg.Database.Name,
		cfg.Database.SslMode,
	)

	db, err := database.NewPostgres(dsn, cfg.Database.Driver)
	if err != nil {
		log.Fatalf("Failed init postgres: %v", err)
	}
	defer db.Close()

	if *migration {
		if err = goose.Up(db.DB, cfg.Database.Migrations); err != nil {
			log.Fatalf("Migration faileds: %v", err)
		}
	}

	categoryRepo := facade.NewCategoryRepo(db)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Println("Facade server starting...")

	configuredTopics := categoryTopics{
		Created: cfg.Kafka.Topics.Created,
		Updated: cfg.Kafka.Topics.Updated,
		Removed: cfg.Kafka.Topics.Removed,
	}

	topicList := []string{
		configuredTopics.Created,
		configuredTopics.Updated,
		configuredTopics.Removed,
	}

	var wg sync.WaitGroup

	for _, topic := range topicList {
		wg.Add(1)

		go func(topic string) {
			defer wg.Done()
			runReader(ctx, cfg.Kafka.Brokers, cfg.Kafka.GroupID, topic, configuredTopics, categoryRepo)
		}(topic)
	}

	<-ctx.Done()
	wg.Wait()

	log.Println("Facade server stopped.")
}

func runReader(ctx context.Context, brokers []string, groupID string, topic string, configuredTopics categoryTopics, categoryRepo facade.CategoryRepo) {
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
				log.Printf("Reader for topic %s stopped: %v", topic, ctx.Err())
				break
			}

			log.Printf("Failed to read message from topic %s: %v", topic, err)
			continue
		}

		err = handleMessage(ctx, msg.Topic, msg.Value, configuredTopics, categoryRepo)

		if err != nil {
			log.Printf(
				"Failed to process message from topic %s, partition %d, offset %d: %v",
				msg.Topic,
				msg.Partition,
				msg.Offset,
				err,
			)

			continue
		}

		log.Printf(
			"Processed message: Topic: %s | Key: %s | Value: %s | Partition: %d | Offset: %d",
			msg.Topic,
			string(msg.Key),
			string(msg.Value),
			msg.Partition,
			msg.Offset,
		)
	}
}

func handleMessage(ctx context.Context, topic string, value []byte, topics categoryTopics, categoryRepo facade.CategoryRepo) error {
	var payload categoryPayload

	if err := json.Unmarshal(value, &payload); err != nil {
		return fmt.Errorf("decode category event: %w", err)
	}

	if payload.CategoryID == 0 {
		return fmt.Errorf("category event contains invalid category_id")
	}

	switch topic {
	case topics.Created:
		if payload.Foo == "" {
			return fmt.Errorf("category event contains invalid foo")
		}

		if err := categoryRepo.Upsert(
			ctx,
			payload.CategoryID,
			payload.Foo,
		); err != nil {
			return fmt.Errorf("process created category event: %w", err)
		}

	case topics.Updated:
		if payload.Foo == "" {
			return fmt.Errorf("category event contains invalid foo")
		}

		if err := categoryRepo.Upsert(ctx, payload.CategoryID, payload.Foo); err != nil {
			return fmt.Errorf("process updated category event: %w", err)
		}

	case topics.Removed:
		if err := categoryRepo.MarkRemoved(ctx, payload.CategoryID); err != nil {
			return fmt.Errorf("process removed category event: %w", err)
		}

	default:
		return fmt.Errorf("unsupported category topic: %s", topic)
	}

	return nil
}
