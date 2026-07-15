package retranslator

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/vvvvgross/catalog-category-api/internal/model"
	"github.com/vvvvgross/catalog-category-api/internal/repo"
)

type EventProducer interface {
	Send(ctx context.Context, event model.CategoryEvent) error
}

type Retranslator struct {
	eventRepo repo.EventRepo
	producer  EventProducer
	limit     uint64
	interval  time.Duration
}

func New(
	eventRepo repo.EventRepo,
	producer EventProducer,
	limit uint64,
	interval time.Duration,
) *Retranslator {
	return &Retranslator{
		eventRepo: eventRepo,
		producer:  producer,
		limit:     limit,
		interval:  interval,
	}
}

func (r *Retranslator) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		if err := r.Process(ctx); err != nil {
			log.Error().Err(err).Msg("failed to process category events")
		}

		select {
		case <-ctx.Done():
			log.Info().Msg("retranslator stopped")
			return
		case <-ticker.C:
		}
	}
}

func (r *Retranslator) Process(ctx context.Context) error {
	events, err := r.eventRepo.Lock(ctx, r.limit)
	if err != nil {
		return err
	}

	if len(events) == 0 {
		return nil
	}

	successIDs := make([]uint64, 0, len(events))
	failedIDs := make([]uint64, 0)

	for _, event := range events {
		if err := r.producer.Send(ctx, event); err != nil {
			log.Error().
				Err(err).
				Uint64("event_id", event.ID).
				Uint64("category_id", event.CategoryID).
				Str("event_type", event.Type).
				Msg("failed to send category event to kafka")

			failedIDs = append(failedIDs, event.ID)

			continue
		}

		log.Debug().
			Uint64("event_id", event.ID).
			Uint64("category_id", event.CategoryID).
			Str("event_type", event.Type).
			Msg("category event sent to kafka")

		successIDs = append(successIDs, event.ID)
	}

	if len(successIDs) > 0 {
		if _, err = r.eventRepo.Remove(ctx, successIDs); err != nil {
			unlockIDs := append(failedIDs, successIDs...)

			if unlockErr := r.eventRepo.Unlock(ctx, unlockIDs); unlockErr != nil {
				log.Error().Err(unlockErr).Msg("failed to unlock events after remove error")
			}

			return err
		}
	}

	if len(failedIDs) > 0 {
		return r.eventRepo.Unlock(ctx, failedIDs)
	}

	return nil
}
