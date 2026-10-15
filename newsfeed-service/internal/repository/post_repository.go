package repository

import (
	"context"

	"github.com/doug-martin/goqu/v9"
	"github.com/namnv2496/newsfeed-service/internal/domain"
)

type PostRepository interface {
	GetPosts(context.Context, PostFilter) ([]domain.Post, error)
}

type PostFilter struct {
	IDs         []uint64
	VisibleOnly bool
}

type postRepository struct {
	db *goqu.Database
}

func NewPostRepository(db *goqu.Database) PostRepository {
	return &postRepository{db: db}
}

func (r postRepository) GetPosts(ctx context.Context, filter PostFilter) ([]domain.Post, error) {
	query := r.db.From(domain.TabNamePost)
	if len(filter.IDs) > 0 {
		query = query.Where(goqu.C("id").In(filter.IDs))
	}
	if filter.VisibleOnly {
		query = query.Where(goqu.C("visible").Eq(true))
	}

	var posts []domain.Post
	err := query.Executor().ScanStructsContext(ctx, &posts)
	return posts, err
}
