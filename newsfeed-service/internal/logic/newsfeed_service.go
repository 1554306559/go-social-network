package logic

import (
	"context"
	"sort"
	"strconv"
	"strings"

	"github.com/namnv2496/newsfeed-service/internal/cache"
	"github.com/namnv2496/newsfeed-service/internal/domain"
	newsfeedv1 "github.com/namnv2496/newsfeed-service/internal/handler/generated/newsfeed_core/v1"
	"github.com/namnv2496/newsfeed-service/internal/mq/consumer"
	"github.com/namnv2496/newsfeed-service/internal/repository"
)

type NewsfeedService interface {
	GetPost(context.Context, uint64) (domain.Post, bool, error)
	UpdateNewsfeed(context.Context, []string, uint64) error
	GetNewsfeed(context.Context, string) (*newsfeedv1.GetNewsfeedResponse, error)
}

type newsfeedService struct {
	postRepo    repository.PostRepository
	kafkaClient consumer.Consumer
	redis       cache.Client
}

func NewUserService(
	postRepo repository.PostRepository,
	kafkaClient consumer.Consumer,
	redis cache.Client,
) NewsfeedService {
	return &newsfeedService{
		postRepo:    postRepo,
		kafkaClient: kafkaClient,
		redis:       redis,
	}
}

func (s newsfeedService) UpdateNewsfeed(
	ctx context.Context,
	users []string,
	postID uint64,
) error {

	if len(users) == 0 {
		return nil
	}
	keys := make([]string, 0, len(users))
	for _, user := range users {
		user = strings.ToLower(user)
		user = strings.Trim(user, " ")
		keys = append(keys, user+"_newsfeed")
	}
	return s.redis.AddToSets(ctx, keys, postID)
}

func (s newsfeedService) GetPost(ctx context.Context, postID uint64) (domain.Post, bool, error) {
	posts, err := s.postRepo.GetPosts(ctx, repository.PostFilter{IDs: []uint64{postID}})
	if err != nil || len(posts) == 0 {
		return domain.Post{}, false, err
	}
	return posts[0], true, nil
}

// 拉去用户的推送列表
func (s newsfeedService) GetNewsfeed(
	ctx context.Context,
	userId string,
) (*newsfeedv1.GetNewsfeedResponse, error) {
	key := strings.ToLower(strings.Trim(userId, " ")) + "_newsfeed"
	members, err := s.redis.GetSetMembers(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return &newsfeedv1.GetNewsfeedResponse{Posts: []*newsfeedv1.NewsfeedPost{}}, nil
	}

	postIDs := make([]uint64, 0, len(members))
	for _, member := range members {
		postID, err := strconv.ParseUint(member, 10, 64)
		if err != nil {
			return nil, err
		}
		postIDs = append(postIDs, postID)
	}

	posts, err := s.postRepo.GetPosts(ctx, repository.PostFilter{
		IDs:         postIDs,
		VisibleOnly: true,
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(posts, func(i, j int) bool {
		return posts[i].CreatedAt.After(posts[j].CreatedAt)
	})

	postPointers := make([]*newsfeedv1.NewsfeedPost, 0, len(posts))
	for _, post := range posts {
		postPointers = append(postPointers, &newsfeedv1.NewsfeedPost{
			UserId:      post.User_id,
			PostId:      post.Id,
			ContentText: post.Content_text,
			Images:      strings.Split(post.Images, ","),
			Tags:        strings.Split(post.Tags, ","),
			Visible:     post.Visible,
			Date:        post.CreatedAt.String(),
		})
	}
	return &newsfeedv1.GetNewsfeedResponse{
		Posts: postPointers,
	}, nil
}
