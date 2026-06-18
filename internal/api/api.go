package api

import (
	"context"
	"database/sql"
	"errors"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
	log.Debug().Str("foo", req.GetFoo()).
		Msg("CreateCategoryV1 called")

	err := req.Validate()
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	category := &model.Category{
		Foo: req.GetFoo(),
	}

	categoryID, err := c.repo.Add(ctx, category)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.CreateCategoryV1Response{
		CategoryId: categoryID,
	}, nil
}

func (c *categoryAPI) DescribeCategoryV1(
	ctx context.Context,
	req *pb.DescribeCategoryV1Request,
) (*pb.DescribeCategoryV1Response, error) {
	log.Debug().Uint64("category_id", req.GetCategoryId()).Msg("DescribeCategoryV1 called")

	err := req.Validate()
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	category, err := c.repo.Get(ctx, req.GetCategoryId())
	if errors.Is(err, sql.ErrNoRows) {
		return nil, status.Error(codes.NotFound, err.Error())
	} else if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.DescribeCategoryV1Response{
		Value: categoryToProto(*category),
	}, nil
}

func (c *categoryAPI) ListCategoriesV1(
	ctx context.Context,
	req *pb.ListCategoriesV1Request,
) (*pb.ListCategoriesV1Response, error) {
	log.Debug().Msg("ListCategoriesV1 called")

	categories, err := c.repo.List(ctx, req.GetLimit(), req.GetCursor())

	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	items := make([]*pb.Category, 0, len(categories))
	for _, category := range categories {
		items = append(items, categoryToProto(category))
	}

	return &pb.ListCategoriesV1Response{
		Items: items,
	}, nil
}

func (c *categoryAPI) RemoveCategoryV1(
	ctx context.Context,
	req *pb.RemoveCategoryV1Request,
) (*pb.RemoveCategoryV1Response, error) {
	log.Debug().Uint64("category_id", req.GetCategoryId()).Msg("RemoveCategoryV1 called")

	err := req.Validate()
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	flag, err := c.repo.Remove(ctx, req.GetCategoryId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
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
