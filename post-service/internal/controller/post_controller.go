package controller

import (
	"context"

	postv1 "github.com/namnv2496/post-service/internal/handler/generated/post_core/v1"
	"github.com/namnv2496/post-service/internal/pkg/metric"
	"github.com/opentracing/opentracing-go"
)

// 帖子创建
func (c *Controller) CreatePost(
	ctx context.Context,
	req *postv1.CreatePostRequest,
) (*postv1.CreatePostResponse, error) {
	// 链路追踪，监控
	// 如果原来的 ctx 已经包含上游 Span，它会尝试把新的 Span 建立为子 Span
	span, ctx := opentracing.StartSpanFromContext(ctx, "post.CreatePost")
	defer span.Finish()
	// 统计创建帖子次数
	metric.MetricNewPostCnt("CreatePost")
	if err := c.validatorSvc.Validate(ctx, req); err != nil {
		return nil, err
	}
	return c.postService.AddPost(ctx, req)
}

func (c *Controller) GetPost(
	ctx context.Context,
	req *postv1.GetPostRequest,
) (*postv1.GetPostResponse, error) {
	// span, ctx := opentracing.StartSpanFromContext(ctx, "user.Create")
	// defer span.Finish()
	metric.MetricGetPostCnt("GetPost")
	return c.postService.GetPosts(ctx, req)
}
