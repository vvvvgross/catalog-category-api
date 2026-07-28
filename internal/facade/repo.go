package facade

import (
	"context"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
)

type CategoryRepo interface {
	Upsert(ctx context.Context, categoryID uint64, foo string) error
	MarkRemoved(ctx context.Context, categoryID uint64) error
}

type categoryRepo struct {
	db *sqlx.DB
}

func NewCategoryRepo(db *sqlx.DB) CategoryRepo {
	return &categoryRepo{db: db}
}

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

func (r *categoryRepo) Upsert(ctx context.Context, categoryID uint64, foo string) error {
	const query = `
        INSERT INTO categories (id, foo, removed)
		VALUES ($1, $2, FALSE)
		ON CONFLICT (id)
		DO UPDATE SET
			foo = EXCLUDED.foo,
			removed = FALSE,
			updated = NOW();
    `

	_, err := r.db.ExecContext(ctx, query, categoryID, foo)
	if err != nil {
		return fmt.Errorf("upsert category: %w", err)
	}

	return nil
}

func (r *categoryRepo) MarkRemoved(ctx context.Context, categoryID uint64) error {
	sqlStr, args, err := psql.Update("categories").
		Set("removed", true).
		Set("updated", time.Now()).
		Where(sq.Eq{"id": categoryID}).
		Where(sq.Eq{"removed": false}).
		ToSql()

	if err != nil {
		return fmt.Errorf("build mark removed query: %w", err)
	}

	_, err = r.db.ExecContext(ctx, sqlStr, args...)

	if err != nil {
		return fmt.Errorf("mark category removed: %w", err)
	}

	return nil
}
