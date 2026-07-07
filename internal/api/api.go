package api

import (
	"context"
	"database/sql"
	"errors"

	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	applog "github.com/vvvvgross/catalog-category-api/internal/logger"
	"github.com/vvvvgross/catalog-category-api/internal/model"
	"github.com/vvvvgross/catalog-category-api/internal/repo"
	pb "github.com/vvvvgross/catalog-category-api/pkg/catalog-category-api"
)

var (
	totalCategoryNotFound = promauto.NewCounter(prometheus.CounterOpts{
		Name: "catalog_category_api_category_not_found_total",
		Help: "Total number of categories that were not found",
	})
)

type categoryAPI struct {
	pb.UnimplementedCatalogCategoryApiServiceServer
	repo repo.Repo
}

func NewCategoryAPI(r repo.Repo) pb.CatalogCategoryApiServiceServer {
	return &categoryAPI{
		repo: r,
	}
}

func (c *categoryAPI) CreateCategoryV1(
	ctx context.Context,
	req *pb.CreateCategoryV1Request,
) (*pb.CreateCategoryV1Response, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "CreateCategoryV1")
	defer span.Finish()

	span.SetTag("handler", "CreateCategoryV1")
	span.SetTag("foo", req.GetFoo())

	logger := applog.FromContext(ctx)

	logger.Debug().
		Str("handler", "CreateCategoryV1").
		Str("foo", req.GetFoo()).
		Msg("CreateCategoryV1 called")

	err := req.Validate()
	if err != nil {
		span.SetTag("error", true)
		span.SetTag("validation_error", true)
		span.LogFields(otlog.String("error", err.Error()))

		logger.Warn().
			Err(err).
			Str("handler", "CreateCategoryV1").
			Msg("validation failed")

		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	category := &model.Category{
		Foo: req.GetFoo(),
	}

	categoryID, err := c.repo.Add(ctx, category)
	if err != nil {
		span.SetTag("error", true)
		span.LogFields(otlog.String("error", err.Error()))

		logger.Error().
			Err(err).
			Str("handler", "CreateCategoryV1").
			Str("foo", req.GetFoo()).
			Msg("failed to create category")

		return nil, status.Error(codes.Internal, err.Error())
	}

	span.SetTag("category_id", categoryID)

	logger.Debug().
		Str("handler", "CreateCategoryV1").
		Uint64("category_id", categoryID).
		Msg("category created")

	return &pb.CreateCategoryV1Response{
		CategoryId: categoryID,
	}, nil
}

func (c *categoryAPI) DescribeCategoryV1(
	ctx context.Context,
	req *pb.DescribeCategoryV1Request,
) (*pb.DescribeCategoryV1Response, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "DescribeCategoryV1")
	defer span.Finish()

	span.SetTag("handler", "DescribeCategoryV1")
	span.SetTag("category_id", req.GetCategoryId())

	logger := applog.FromContext(ctx)

	logger.Debug().
		Str("handler", "DescribeCategoryV1").
		Uint64("category_id", req.GetCategoryId()).
		Msg("DescribeCategoryV1 called")

	err := req.Validate()
	if err != nil {
		span.SetTag("error", true)
		span.SetTag("validation_error", true)
		span.LogFields(otlog.String("error", err.Error()))

		logger.Warn().
			Err(err).
			Str("handler", "DescribeCategoryV1").
			Msg("validation failed")

		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	category, err := c.repo.Get(ctx, req.GetCategoryId())
	if errors.Is(err, sql.ErrNoRows) {
		span.SetTag("not_found", true)
		span.LogFields(otlog.String("event", "category not found"))

		logger.Warn().
			Err(err).
			Str("handler", "DescribeCategoryV1").
			Uint64("category_id", req.GetCategoryId()).
			Msg("category not found")

		totalCategoryNotFound.Inc()

		return nil, status.Error(codes.NotFound, err.Error())
	} else if err != nil {
		span.SetTag("error", true)
		span.LogFields(otlog.String("error", err.Error()))

		logger.Error().
			Err(err).
			Str("handler", "DescribeCategoryV1").
			Uint64("category_id", req.GetCategoryId()).
			Msg("failed to describe category")

		return nil, status.Error(codes.Internal, err.Error())
	}

	span.SetTag("found", true)

	logger.Debug().
		Str("handler", "DescribeCategoryV1").
		Uint64("category_id", req.GetCategoryId()).
		Str("foo", category.Foo).
		Msg("category described")

	return &pb.DescribeCategoryV1Response{
		Value: categoryToProto(*category),
	}, nil
}

func (c *categoryAPI) ListCategoriesV1(
	ctx context.Context,
	req *pb.ListCategoriesV1Request,
) (*pb.ListCategoriesV1Response, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "ListCategoriesV1")
	defer span.Finish()

	span.SetTag("handler", "ListCategoriesV1")
	span.SetTag("limit", req.GetLimit())
	span.SetTag("cursor", req.GetCursor())

	logger := applog.FromContext(ctx)

	logger.Debug().
		Str("handler", "ListCategoriesV1").
		Uint64("limit", req.GetLimit()).
		Uint64("cursor", req.GetCursor()).
		Msg("ListCategoriesV1 called")

	categories, err := c.repo.List(ctx, req.GetLimit(), req.GetCursor())

	if err != nil {
		span.SetTag("error", true)
		span.LogFields(otlog.String("error", err.Error()))

		logger.Error().
			Err(err).
			Str("handler", "ListCategoriesV1").
			Msg("failed to list categories")

		return nil, status.Error(codes.Internal, err.Error())
	}

	span.SetTag("categories_count", len(categories))

	items := make([]*pb.Category, 0, len(categories))
	for _, category := range categories {
		items = append(items, categoryToProto(category))
	}

	logger.Debug().
		Str("handler", "ListCategoriesV1").
		Uint64("limit", req.GetLimit()).
		Uint64("cursor", req.GetCursor()).
		Int("items_count", len(items)).
		Msg("categories listed")

	return &pb.ListCategoriesV1Response{
		Items: items,
	}, nil
}

func (c *categoryAPI) RemoveCategoryV1(
	ctx context.Context,
	req *pb.RemoveCategoryV1Request,
) (*pb.RemoveCategoryV1Response, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "RemoveCategoryV1")
	defer span.Finish()

	span.SetTag("handler", "RemoveCategoryV1")
	span.SetTag("category_id", req.GetCategoryId())

	logger := applog.FromContext(ctx)

	logger.Debug().
		Str("handler", "RemoveCategoryV1").
		Uint64("category_id", req.GetCategoryId()).
		Msg("RemoveCategoryV1 called")

	err := req.Validate()
	if err != nil {
		span.SetTag("error", true)
		span.SetTag("validation_error", true)
		span.LogFields(otlog.String("error", err.Error()))

		logger.Warn().
			Err(err).
			Str("handler", "RemoveCategoryV1").
			Msg("validation failed")

		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	flag, err := c.repo.Remove(ctx, req.GetCategoryId())
	if err != nil {
		span.SetTag("error", true)
		span.LogFields(otlog.String("error", err.Error()))

		logger.Error().
			Err(err).
			Str("handler", "RemoveCategoryV1").
			Uint64("category_id", req.GetCategoryId()).
			Msg("failed to remove category")

		return nil, status.Error(codes.Internal, err.Error())
	}

	if !flag {
		span.SetTag("not_found", true)
		span.LogFields(otlog.String("event", "category not found"))

		logger.Warn().
			Str("handler", "RemoveCategoryV1").
			Uint64("category_id", req.GetCategoryId()).
			Bool("found", flag).
			Msg("category not found")

		totalCategoryNotFound.Inc()
	} else {
		span.SetTag("removed", true)

		logger.Debug().
			Str("handler", "RemoveCategoryV1").
			Uint64("category_id", req.GetCategoryId()).
			Bool("found", flag).
			Msg("category removed")
	}

	return &pb.RemoveCategoryV1Response{
		Found: flag,
	}, nil
}

func categoryToProto(category model.Category) *pb.Category {
	return &pb.Category{
		Id:  category.ID,
		Foo: category.Foo,
	}
}
