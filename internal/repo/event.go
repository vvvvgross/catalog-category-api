package repo

import (
	"context"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/vvvvgross/catalog-category-api/internal/model"
)

func (r *eventRepo) Lock(ctx context.Context, n uint64) ([]model.CategoryEvent, error) {
	if n == 0 {
		return []model.CategoryEvent{}, nil
	}

	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
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
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return events, nil
}

func (r *eventRepo) Unlock(ctx context.Context, eventIDs []uint64) error {
	if len(eventIDs) == 0 {
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
		return err
	}

	_, err = r.db.ExecContext(ctx, sqlStr, args...)

	return err
}

func (r *eventRepo) Remove(ctx context.Context, eventIDs []uint64) (bool, error) {
	if len(eventIDs) == 0 {
		return false, nil
	}

	sqlStr, args, err := psql.Delete("categories_events").
		Where(sq.Eq{"id": eventIDs}).
		ToSql()

	if err != nil {
		return false, err
	}

	result, err := r.db.ExecContext(ctx, sqlStr, args...)
	if err != nil {
		return false, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}

	return rowsAffected > 0, nil
}
