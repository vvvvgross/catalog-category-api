package repo

import (
	"context"

	"github.com/jmoiron/sqlx"

	"github.com/vvvvgross/catalog-category-api/internal/model"
)

type Repo interface {
	DescribeCategory(ctx context.Context, categoryID uint64) (*model.Category, error)
}

type repo struct {
	db        *sqlx.DB
	batchSize uint
}

// NewRepo returns Repo interface
func NewRepo(db *sqlx.DB, batchSize uint) Repo {
	return &repo{db: db, batchSize: batchSize}
}

func (r *repo) DescribeCategory(ctx context.Context, categoryID uint64) (*model.Category, error) {
	return nil, nil
}
