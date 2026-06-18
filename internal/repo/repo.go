package repo

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
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
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	sqlStr, args, err := psql.Insert("categories").
		Columns("foo").
		Values(category.Foo).
		Suffix("RETURNING id").
		ToSql()

	if err != nil {
		return 0, err
	}

	err = tx.QueryRowContext(ctx, sqlStr, args...).Scan(&category.ID)
	if err != nil {
		return 0, err
	}

	payload := categoryPayload{
		CategoryID: category.ID,
		Foo:        category.Foo,
	}

	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}

	sqlStr, args, err = psql.Insert("categories_events").
		Columns("category_id", "type", "status", "payload").
		Values(category.ID, model.CategoryEventTypeCreated, model.CategoryEventStatusPending, jsonBytes).
		ToSql()

	if err != nil {
		return 0, err
	}

	_, err = tx.ExecContext(ctx, sqlStr, args...)
	if err != nil {
		return 0, err
	}

	err = tx.Commit()
	if err != nil {
		return 0, err
	}

	return category.ID, nil
}

func (r *repo) Get(ctx context.Context, categoryID uint64) (*model.Category, error) {
	sqlStr, args, err := psql.Select("id", "foo", "removed", "created", "updated").
		From("categories").
		Where(sq.Eq{
			"id":      categoryID,
			"removed": false,
		}).
		ToSql()

	if err != nil {
		return nil, err
	}

	var category model.Category
	err = r.db.GetContext(ctx, &category, sqlStr, args...)

	if err != nil {
		return nil, err
	}

	return &category, nil
}

func (r *repo) List(ctx context.Context, limit uint64, cursor uint64) ([]model.Category, error) {
	sqlStr, args, err := psql.Select("id", "foo", "removed", "created", "updated").
		From("categories").
		Where(sq.Eq{"removed": false}).
		Where(sq.Gt{"id": cursor}).
		OrderBy("id").
		Limit(limit).
		ToSql()

	if err != nil {
		return nil, err
	}

	var categories []model.Category
	err = r.db.SelectContext(ctx, &categories, sqlStr, args...)

	if err != nil {
		return nil, err
	}

	return categories, nil
}

func (r *repo) Remove(ctx context.Context, categoryID uint64) (bool, error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
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
		return false, err
	}

	result := tx.QueryRowContext(ctx, sqlStr, args...)
	var returnID uint64
	var returnFOO string
	err = result.Scan(&returnID, &returnFOO)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	payload := categoryPayload{
		CategoryID: returnID,
		Foo:        returnFOO,
	}

	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return false, err
	}

	sqlStr, args, err = psql.Insert("categories_events").
		Columns("category_id", "type", "status", "payload").
		Values(returnID, model.CategoryEventTypeRemoved, model.CategoryEventStatusPending, jsonBytes).
		ToSql()

	if err != nil {
		return false, err
	}

	_, err = tx.ExecContext(ctx, sqlStr, args...)
	if err != nil {
		return false, err
	}

	err = tx.Commit()
	if err != nil {
		return false, err
	}

	return true, nil
}
