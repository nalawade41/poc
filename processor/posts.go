package processor

import (
	"context"
	"fmt"
	"time"

	"github.com/nalawade41/poc/types"
	"github.com/nalawade41/poc/util"
	"github.com/vartanbeno/go-reddit/v2/reddit"
)

type PostProcessor struct {
	RedditClient *reddit.Client
	JobQueue     chan types.Job
	Operator     *Operator
}

func NewPostProcessor(client *reddit.Client, jobQueue chan types.Job, operator *Operator) *PostProcessor {
	return &PostProcessor{
		RedditClient: client,
		JobQueue:     jobQueue,
		Operator:     operator,
	}
}

// ProcessPosts iterates over posts and processes each individually.
func (p *PostProcessor) ProcessPosts(ctx context.Context, subreddit string, keywords []string) error {
	posts, err := p.fetchPosts(ctx, subreddit)
	if err != nil {
		return fmt.Errorf("error fetching posts: %w", err)
	}

	if len(posts) == 0 {
		return nil
	}

	var batchPosts []*reddit.Post
	var counter int
	for _, post := range posts {
		counter++
		p.processSinglePost(ctx, post, keywords, &batchPosts)
		p.Operator.UpdatePostTracking(ctx, post.ID, subreddit, time.Now(), time.Now(), post.Edited.Time, post.NumberOfComments)
	}
	fmt.Printf("Processed %d posts for subreddit: %s\n", counter, subreddit)

	if len(batchPosts) > 0 {
		p.Operator.SavePostsToDatabase(ctx, batchPosts)
	}

	return nil
}

// processSinglePost processes an individual post, checking for keywords and scheduling comment jobs if needed.
func (p *PostProcessor) processSinglePost(ctx context.Context, post *reddit.Post, keywords []string, batchPosts *[]*reddit.Post) {
	p.checkAndAddPostToBatch(ctx, post, keywords, batchPosts)
	p.enqueueCommentsJobIfNeeded(ctx, post, keywords)
}

// checkAndAddPostToBatch checks if a post contains the keywords and adds it to the batch if it does.
func (p *PostProcessor) checkAndAddPostToBatch(ctx context.Context, post *reddit.Post, keywords []string, batchPosts *[]*reddit.Post) bool {
	postNeedsReprocessing, _ := p.Operator.ShouldReprocessPost(ctx, post.ID, post.Edited.Time, post.NumberOfComments)
	if !postNeedsReprocessing {
		return false
	}

	for _, keyword := range keywords {
		if util.ContainsKeyword(post.Title, keyword) || util.ContainsKeyword(post.Body, keyword) {
			fmt.Printf("Found keyword '%s' in post: %s\n", keyword, post.Title)
			*batchPosts = append(*batchPosts, post)
			return true
		}
	}
	return false
}

// enqueueCommentsJobIfNeeded adds a job to fetch comments if the post has new comments.
func (p *PostProcessor) enqueueCommentsJobIfNeeded(ctx context.Context, post *reddit.Post, keywords []string) bool {
	_, commentsNeedFetching := p.Operator.ShouldReprocessPost(ctx, post.ID, post.Edited.Time, post.NumberOfComments)
	if commentsNeedFetching && post.NumberOfComments > 0 {
		// Add the job to the queue
		util.IncrementActiveJobs(post.SubredditName)

		p.JobQueue <- types.Job{
			RequestType: "Comments",
			Post:        post,
			Keywords:    keywords,
		}
		return true
	}
	return false
}

func (p *PostProcessor) fetchPosts(ctx context.Context, subreddit string) ([]*reddit.Post, error) {
	posts, _, err := p.RedditClient.Subreddit.NewPosts(ctx, subreddit, &reddit.ListOptions{Limit: 100})
	if err != nil {
		return nil, fmt.Errorf("error fetching posts: %w", err)
	}

	//TODO: Add hot posts, trending post and other types of posts and return them as a single list
	return posts, nil
}
