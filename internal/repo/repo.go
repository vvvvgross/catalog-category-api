package repo

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
	"github.com/vvvvgross/catalog-category-api/internal/model"
)

type Repo interface {
	Add(ctx context.Context, category *model.Category) (uint64, error)
	Get(ctx context.Context, categoryID uint64) (*model.Category, error)
	List(ctx context.Context, limit uint64, cursor uint64) ([]model.Category, error)
	Remove(ctx context.Context, categoryID uint64) (bool, error)
}

type EventRepo interface {
	Lock(ctx context.Context, n uint64) ([]model.CategoryEvent, error)
	Unlock(ctx context.Context, eventIDs []uint64) error
	Remove(ctx context.Context, eventIDs []uint64) (bool, error)
}

type repo struct {
	db        *sqlx.DB
	batchSize uint
}

type eventRepo struct {
	db        *sqlx.DB
	batchSize uint
}

type categoryPayload struct {
	CategoryID uint64 `json:"category_id"`
	Foo        string `json:"foo"`
}

func NewRepo(db *sqlx.DB, batchSize uint) Repo {
	return &repo{db: db, batchSize: batchSize}
}

func NewEventRepo(db *sqlx.DB, batchSize uint) EventRepo {
	return &eventRepo{db: db, batchSize: batchSize}
}

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

func (r *repo) Add(ctx context.Context, category *model.Category) (uint64, error) {
	log.Debug().
		Str("repo", "category").
		Str("method", "Add").
		Str("foo", category.Foo).
		Msg("adding category")

	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category").
			Str("method", "Add").
			Msg("failed to begin transaction")

		return 0, err
	}
	defer tx.Rollback()

	sqlStr, args, err := psql.Insert("categories").
		Columns("foo").
		Values(category.Foo).
		Suffix("RETURNING id").
		ToSql()

	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category").
			Str("method", "Add").
			Msg("failed to build insert category query")

		return 0, err
	}

	err = tx.QueryRowContext(ctx, sqlStr, args...).Scan(&category.ID)
	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category").
			Str("method", "Add").
			Str("foo", category.Foo).
			Msg("failed to insert category")

		return 0, err
	}

	payload := categoryPayload{
		CategoryID: category.ID,
		Foo:        category.Foo,
	}

	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category").
			Str("method", "Add").
			Str("foo", category.Foo).
			Msg("failed to marshal category event payload")

		return 0, err
	}

	sqlStr, args, err = psql.Insert("categories_events").
		Columns("category_id", "type", "status", "payload").
		Values(category.ID, model.CategoryEventTypeCreated, model.CategoryEventStatusPending, jsonBytes).
		ToSql()

	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category").
			Str("method", "Add").
			Uint64("category_id", category.ID).
			Msg("failed to build insert category event query")

		return 0, err
	}

	_, err = tx.ExecContext(ctx, sqlStr, args...)
	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category").
			Str("method", "Add").
			Uint64("category_id", category.ID).
			Str("event_type", string(model.CategoryEventTypeCreated)).
			Msg("failed to insert category event")

		return 0, err
	}

	err = tx.Commit()
	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category").
			Str("method", "Add").
			Uint64("category_id", category.ID).
			Str("event_type", string(model.CategoryEventTypeCreated)).
			Msg("failed to commit transaction")

		return 0, err
	}

	log.Debug().
		Str("repo", "category").
		Str("method", "Add").
		Uint64("category_id", category.ID).
		Str("event_type", string(model.CategoryEventTypeCreated)).
		Msg("category added")

	return category.ID, nil
}

func (r *repo) Get(ctx context.Context, categoryID uint64) (*model.Category, error) {
	log.Debug().
		Str("repo", "category").
		Str("method", "Get").
		Uint64("category_id", categoryID).
		Msg("getting category")

	sqlStr, args, err := psql.Select("id", "foo", "removed", "created", "updated").
		From("categories").
		Where(sq.Eq{
			"id":      categoryID,
			"removed": false,
		}).
		ToSql()

	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category").
			Str("method", "Get").
			Uint64("category_id", categoryID).
			Msg("failed to build get category query")

		return nil, err
	}

	var category model.Category
	err = r.db.GetContext(ctx, &category, sqlStr, args...)

	if errors.Is(err, sql.ErrNoRows) {
		log.Debug().
			Err(err).
			Str("repo", "category").
			Str("method", "Get").
			Uint64("category_id", categoryID).
			Msg("category not found")

		return nil, err
	}

	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category").
			Str("method", "Get").
			Uint64("category_id", categoryID).
			Msg("failed to get category")

		return nil, err
	}

	log.Debug().
		Str("repo", "category").
		Str("method", "Get").
		Uint64("category_id", category.ID).
		Msg("category found")

	return &category, nil
}

func (r *repo) List(ctx context.Context, limit uint64, cursor uint64) ([]model.Category, error) {
	log.Debug().
		Str("repo", "category").
		Str("method", "List").
		Uint64("limit", limit).
		Uint64("cursor", cursor).
		Msg("listing categories")

	sqlStr, args, err := psql.Select("id", "foo", "removed", "created", "updated").
		From("categories").
		Where(sq.Eq{"removed": false}).
		Where(sq.Gt{"id": cursor}).
		OrderBy("id").
		Limit(limit).
		ToSql()

	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category").
			Str("method", "List").
			Uint64("limit", limit).
			Uint64("cursor", cursor).
			Msg("failed to build list categories query")

		return nil, err
	}

	var categories []model.Category
	err = r.db.SelectContext(ctx, &categories, sqlStr, args...)

	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category").
			Str("method", "List").
			Uint64("limit", limit).
			Uint64("cursor", cursor).
			Msg("failed build listing categories request")

		return nil, err
	}

	log.Debug().
		Str("repo", "category").
		Str("method", "List").
		Uint64("limit", limit).
		Uint64("cursor", cursor).
		Int("items_count", len(categories)).
		Msg("categories listed")

	return categories, nil
}

func (r *repo) Remove(ctx context.Context, categoryID uint64) (bool, error) {
	log.Debug().
		Str("repo", "category").
		Str("method", "Remove").
		Uint64("category_id", categoryID).
		Msg("removing category")

	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category").
			Str("method", "Remove").
			Uint64("category_id", categoryID).
			Msg("failed to begin transaction")

		return false, err
	}
	defer tx.Rollback()

	sqlStr, args, err := psql.Update("categories").
		Set("removed", true).
		Set("updated", time.Now()).
		Where(sq.Eq{"id": categoryID}).
		Where(sq.Eq{"removed": false}).
		Suffix("RETURNING id, foo").
		ToSql()

	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category").
			Str("method", "Remove").
			Uint64("category_id", categoryID).
			Msg("failed to build remove category query")

		return false, err
	}

	result := tx.QueryRowContext(ctx, sqlStr, args...)
	var returnID uint64
	var returnFOO string
	err = result.Scan(&returnID, &returnFOO)
	if errors.Is(err, sql.ErrNoRows) {
		log.Debug().
			Str("repo", "category").
			Str("method", "Remove").
			Uint64("category_id", categoryID).
			Bool("found", false).
			Msg("category not found")

		return false, nil
	} else if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category").
			Str("method", "Remove").
			Uint64("category_id", categoryID).
			Msg("failed to remove category")

		return false, err
	}

	payload := categoryPayload{
		CategoryID: returnID,
		Foo:        returnFOO,
	}

	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category").
			Str("method", "Remove").
			Uint64("category_id", categoryID).
			Msg("failed to marshal category event payload")

		return false, err
	}

	sqlStr, args, err = psql.Insert("categories_events").
		Columns("category_id", "type", "status", "payload").
		Values(returnID, model.CategoryEventTypeRemoved, model.CategoryEventStatusPending, jsonBytes).
		ToSql()

	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category").
			Str("method", "Remove").
			Uint64("category_id", categoryID).
			Msg("failed to build insert category event query")

		return false, err
	}

	_, err = tx.ExecContext(ctx, sqlStr, args...)
	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category").
			Str("method", "Remove").
			Uint64("category_id", categoryID).
			Msg("failed to insert category event")

		return false, err
	}

	err = tx.Commit()
	if err != nil {
		log.Error().
			Err(err).
			Str("repo", "category").
			Str("method", "Remove").
			Uint64("category_id", categoryID).
			Msg("failed to commit transaction")

		return false, err
	}

	log.Debug().
		Str("repo", "category").
		Str("method", "Remove").
		Uint64("category_id", returnID).
		Bool("found", true).
		Str("event_type", string(model.CategoryEventTypeRemoved)).
		Msg("category removed")

	return true, nil
}
