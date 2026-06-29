package repo

import (
	"context"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/rs/zerolog/log"
	"github.com/vvvvgross/catalog-category-api/internal/model"
)

func (r *eventRepo) Lock(ctx context.Context, n uint64) ([]model.CategoryEvent, error) {
	log.Debug().
		Str("repo", "category_event").
		Str("method", "Lock").
		Uint64("limit", n).
		Msg("locking events")

	if n == 0 {
		log.Debug().
			Str("repo", "category_event").
			Str("method", "Lock").
			Uint64("limit", n).
			Int("events_count", 0).
			Msg("no events to lock")

		return []model.CategoryEvent{}, nil
	}

	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category_event").
			Str("method", "Lock").
			Uint64("limit", n).
			Msg("failed to begin transaction")

		return nil, err
	}
	defer tx.Rollback()

	query := `WITH locked_events AS (
	SELECT id
	FROM categories_events
	WHERE status = $1
	ORDER BY updated, id
	LIMIT $2
	FOR UPDATE SKIP LOCKED)
	UPDATE categories_events
	SET status = $3,
	updated = NOW()
	WHERE id IN (SELECT id FROM locked_events)
	RETURNING id, category_id, type, status, payload, updated;`

	var events []model.CategoryEvent

	err = tx.SelectContext(ctx, &events, query, model.CategoryEventStatusPending,
		n, model.CategoryEventStatusLocked)
	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category_event").
			Str("method", "Lock").
			Uint64("limit", n).
			Int("events_count", len(events)).
			Msg("failed to lock events")

		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category_event").
			Str("method", "Lock").
			Uint64("limit", n).
			Int("events_count", len(events)).
			Msg("failed to commit transaction")

		return nil, err
	}

	log.Debug().
		Str("repo", "category_event").
		Str("method", "Lock").
		Uint64("limit", n).
		Int("events_count", len(events)).
		Msg("events locked")

	return events, nil
}

func (r *eventRepo) Unlock(ctx context.Context, eventIDs []uint64) error {
	log.Debug().
		Str("repo", "category_event").
		Str("method", "Unlock").
		Int("event_ids_count", len(eventIDs)).
		Msg("unlocking events")

	if len(eventIDs) == 0 {
		log.Debug().
			Str("repo", "category_event").
			Str("method", "Unlock").
			Int("events_count", 0).
			Msg("no events to unlock")

		return nil
	}

	sqlStr, args, err := psql.Update("categories_events").
		Set("status", model.CategoryEventStatusPending).
		Set("updated", time.Now()).
		Where(sq.Eq{
			"id":     eventIDs,
			"status": model.CategoryEventStatusLocked,
		}).
		ToSql()

	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category_event").
			Str("method", "Unlock").
			Int("event_ids_count", len(eventIDs)).
			Msg("failed to build update events query")

		return err
	}

	_, err = r.db.ExecContext(ctx, sqlStr, args...)

	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category_event").
			Str("method", "Unlock").
			Int("event_ids_count", len(eventIDs)).
			Msg("failed to unlock events")

		return err
	}

	log.Debug().
		Str("repo", "category_event").
		Str("method", "Unlock").
		Int("event_ids_count", len(eventIDs)).
		Msg("events unlocked")

	return nil
}

func (r *eventRepo) Remove(ctx context.Context, eventIDs []uint64) (bool, error) {
	log.Debug().
		Str("repo", "category_event").
		Str("method", "Remove").
		Int("event_ids_count", len(eventIDs)).
		Msg("removing events")

	if len(eventIDs) == 0 {
		log.Debug().
			Str("repo", "category_event").
			Str("method", "Remove").
			Int("events_count", 0).
			Msg("no events to remove")

		return false, nil
	}

	sqlStr, args, err := psql.Delete("categories_events").
		Where(sq.Eq{"id": eventIDs}).
		ToSql()

	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category_event").
			Str("method", "Remove").
			Int("event_ids_count", len(eventIDs)).
			Msg("failed to build delete events query")

		return false, err
	}

	result, err := r.db.ExecContext(ctx, sqlStr, args...)
	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category_event").
			Str("method", "Remove").
			Int("event_ids_count", len(eventIDs)).
			Msg("failed to remove events")

		return false, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category_event").
			Str("method", "Remove").
			Int("event_ids_count", len(eventIDs)).
			Msg("failed to get removed events count")

		return false, err
	}

	removed := rowsAffected > 0

	log.Debug().
		Str("repo", "category_event").
		Str("method", "Remove").
		Int("event_ids_count", len(eventIDs)).
		Int64("rows_affected", rowsAffected).
		Bool("removed", removed).
		Msg("events removed")

	return removed, nil
}
