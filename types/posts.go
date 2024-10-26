package types

import (
	"time"

	"github.com/vartanbeno/go-reddit/v2/reddit"
)

// TrackedPost MongoDB schema: tracked_posts
type TrackedPost struct {
	PostID           string    `bson:"post_id"`
	Subreddit        string    `bson:"subreddit"`
	LastProcessed    time.Time `bson:"last_processed"`
	LastCommentCheck time.Time `bson:"last_comment_check"`
	LastPostEdit     time.Time `bson:"last_post_edit"`
	NumComments      int       `bson:"num_comments"` // Track the number of comments
}

// Job Define a job type for the request queue
type Job struct {
	RequestType string       // "NewPosts" or "Comments"
	Subreddit   string       // Subreddit to fetch posts from
	Post        *reddit.Post // Post for fetching comments
	Keywords    []string     // Keywords to monitor
	RetryCount  int          // Number of retries for the job
}

// PostDocument Define a MongoDB schema for a Post document
type PostDocument struct {
	PostID      string    `bson:"post_id"`
	Title       string    `bson:"title"`
	Body        string    `bson:"body"`
	Subreddit   string    `bson:"subreddit"`
	CreatedAt   time.Time `bson:"created_at"`
	UpdatedAt   time.Time `bson:"updated_at"`
	NumComments int       `bson:"num_comments"`
}

// CommentDocument Define a MongoDB schema for a Comment document
type CommentDocument struct {
	CommentID string    `bson:"comment_id"`
	PostID    string    `bson:"post_id"`
	Body      string    `bson:"body"`
	Author    string    `bson:"author"`
	CreatedAt time.Time `bson:"created_at"`
}
