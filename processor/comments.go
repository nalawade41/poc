package processor

import (
	"context"
	"fmt"

	"github.com/nalawade41/poc/util"
	"github.com/vartanbeno/go-reddit/v2/reddit"
)

type CommentsProcessor struct {
	RedditClient *reddit.Client
	Operator     *Operator
}

func NewCommentsProcessor(client *reddit.Client, operator *Operator) *CommentsProcessor {
	return &CommentsProcessor{
		RedditClient: client,
		Operator:     operator,
	}
}

// ProcessComments handles fetching, filtering, and saving comments for a post.
func (c *CommentsProcessor) ProcessComments(ctx context.Context, post *reddit.Post, keywords []string) error {
	comments, err := c.fetchComments(ctx, post)
	if err != nil {
		return err
	}

	if len(comments) == 0 {
		return nil
	}

	fmt.Printf("Fetched %d comments for post: %s\n", len(comments), post.Title)

	batchComments := c.filterCommentsByKeywords(comments, keywords)

	if len(batchComments) > 0 {
		c.Operator.SaveCommentsToDatabase(ctx, comments)
		fmt.Printf("Saved %d comments to database\n", len(batchComments))
	}

	return nil
}

// fetchComments retrieves comments for the given post.
func (c *CommentsProcessor) fetchComments(ctx context.Context, post *reddit.Post) ([]*reddit.Comment, error) {
	thread, _, err := c.RedditClient.Post.Get(ctx, post.ID)
	if err != nil {
		return nil, fmt.Errorf("error fetching comments: %w", err)
	}
	return thread.Comments, nil
}

// filterCommentsByKeywords filters comments containing any of the specified keywords.
func (c *CommentsProcessor) filterCommentsByKeywords(comments []*reddit.Comment, keywords []string) []*reddit.Comment {
	var filteredComments []*reddit.Comment
	for _, comment := range comments {
		if c.containsAnyKeyword(comment.Body, keywords) {
			fmt.Printf("Found keyword in comment: %s\n", comment.Body)
			filteredComments = append(filteredComments, comment)
		}
	}
	return filteredComments
}

// containsAnyKeyword checks if the comment contains any of the specified keywords.
func (c *CommentsProcessor) containsAnyKeyword(text string, keywords []string) bool {
	for _, keyword := range keywords {
		if util.ContainsKeyword(text, keyword) {
			return true
		}
	}
	return false
}
