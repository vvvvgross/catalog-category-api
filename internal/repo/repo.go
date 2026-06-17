package repo

import (
	"context"
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

func NewRepo(db *sqlx.DB, batchSize uint) Repo {
	return &repo{db: db, batchSize: batchSize}
}

func NewEventRepo(db *sqlx.DB, batchSize uint) EventRepo {
	return &eventRepo{db: db, batchSize: batchSize}
}

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

func (r *repo) Add(ctx context.Context, category *model.Category) (uint64, error) {
	sqlStr, args, err := psql.Insert("categories").
		Columns("foo").
		Values(category.Foo).
		Suffix("RETURNING id").
		ToSql()

	if err != nil {
		return 0, err
	}

	err = r.db.QueryRowContext(ctx, sqlStr, args...).Scan(&category.ID)

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
	sqlStr, args, err := psql.Update("categories").
		Set("removed", true).
		Set("updated", time.Now()).
		Where(sq.Eq{"id": categoryID}).
		Where(sq.Eq{"removed": false}).
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
