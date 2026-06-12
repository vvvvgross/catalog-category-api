package repo

import (
	"context"

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

func NewRepo(db *sqlx.DB, batchSize uint) Repo {
	return &repo{db: db, batchSize: batchSize}
}

func (r *repo) Add(ctx context.Context, category *model.Category) (uint64, error) {
	return 0, nil
}

func (r *repo) Get(ctx context.Context, categoryID uint64) (*model.Category, error) {
	return nil, nil
}

func (r *repo) List(ctx context.Context, limit uint64, cursor uint64) ([]model.Category, error) {
	return nil, nil
}

func (r *repo) Remove(ctx context.Context, categoryID uint64) (bool, error) {
	return false, nil
}
