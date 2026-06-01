package api

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

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
