package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vartanbeno/go-reddit/v2/reddit"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const maxRequestsPerMinute = 95
const workerCount = 5                  // Number of workers in the pool
const pollInterval = 600 * time.Second // Adjust the polling interval as necessary
const maxRetries = 3                   // Maximum retry attempts for failed jobs

var rateLimiter = time.Tick(time.Minute / maxRequestsPerMinute)
var trackedPostsCollection *mongo.Collection
var postsCollection *mongo.Collection
var commentsCollection *mongo.Collection
var activeJobs sync.Map

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

func main() {
	// Connect to MongoDB
	var err error
	mongoClient, err := mongo.Connect(context.TODO(), options.Client().ApplyURI("mongodb://admin:password@localhost:27017"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = mongoClient.Disconnect(context.TODO()) }()

	// Ping MongoDB to ensure connection
	err = mongoClient.Ping(context.TODO(), readpref.Primary())
	if err != nil {
		log.Fatal("Could not connect to MongoDB:", err)
	}

	// Get the collection where we will store tracked posts
	trackedPostsCollection = mongoClient.Database("reddit_tracker").Collection("tracked_posts")
	postsCollection = trackedPostsCollection.Database().Collection("posts")
	commentsCollection = trackedPostsCollection.Database().Collection("comments")

	// Example subreddits to monitor
	subreddits := []string{"golang", "programming"}
	keywords := []string{"Goroutine", "Channel", "Concurrency"}

	// Create a job queue and start the worker pool
	jobQueue := make(chan Job, 100) // Buffer size of 100 jobs
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker(jobQueue, &wg)
	}

	// Create context to allow graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Schedule fetching new posts at regular intervals for each subreddit
	for _, subreddit := range subreddits {
		go scheduleNewPostsFetching(ctx, jobQueue, subreddit, keywords)
	}

	// Wait for all workers to finish
	wg.Wait()
}

// Worker function to process jobs from the job queue
func worker(jobQueue chan Job, wg *sync.WaitGroup) {
	defer wg.Done()
	client := reddit.DefaultClient()

	for job := range jobQueue {
		<-rateLimiter // Global rate limiter to control request rate

		var err error
		switch job.RequestType {
		case "NewPosts":
			err = processNewPosts(job.Subreddit, job.Keywords, client, jobQueue)
			if err == nil {
				decrementActiveJobs(job.Subreddit)
			} else {
				job.RetryCount++
				if job.RetryCount >= maxRetries {
					fmt.Printf("Max retries reached for NewPosts job in subreddit %s. Skipping...\n", job.Subreddit)
					decrementActiveJobs(job.Subreddit)
				} else {
					fmt.Printf("Error processing NewPosts job for subreddit %s: %v. Retrying (%d/%d)...\n",
						job.Subreddit, err, job.RetryCount, maxRetries)
					jobQueue <- job // Requeue the job on failure
				}
			}
		case "Comments":
			err = processComments(job.Post, job.Keywords, client)
			if err == nil {
				decrementActiveJobs(job.Post.SubredditName)
			} else {
				job.RetryCount++
				if job.RetryCount >= maxRetries {
					fmt.Printf("Max retries reached for Comments job in subreddit %s. Skipping...\n", job.Post.SubredditName)
					decrementActiveJobs(job.Post.SubredditName)
				} else {
					fmt.Printf("Error processing Comments job for subreddit %s: %v. Retrying (%d/%d)...\n",
						job.Post.SubredditName, err, job.RetryCount, maxRetries)
					jobQueue <- job // Requeue the job on failure
				}
			}
		}
	}
}

// Schedule fetching new posts for a subreddit at regular intervals
func scheduleNewPostsFetching(ctx context.Context, jobQueue chan Job, subreddit string, keywords []string) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !incrementActiveJobs(subreddit) {
				continue // Skip if there's already an active job for this subreddit
			}

			// Enqueue a NewPosts job for this subreddit
			jobQueue <- Job{
				RequestType: "NewPosts",
				Subreddit:   subreddit,
				Keywords:    keywords,
			}
		}
	}
}

// Process new posts and enqueue jobs for fetching comments if needed
func processNewPosts(subreddit string, keywords []string, client *reddit.Client, jobQueue chan Job) error {
	posts, _, err := client.Subreddit.NewPosts(context.Background(), subreddit, &reddit.ListOptions{Limit: 100})
	if err != nil {
		return fmt.Errorf("error fetching posts: %w", err)
	}
	fmt.Printf("Fetched %d posts from subreddit: %s\n", len(posts), subreddit)

	var batchPosts []*reddit.Post
	for _, post := range posts {
		postEditTime := post.Edited.Time
		numComments := post.NumberOfComments

		postNeedsReprocessing, commentsNeedFetching := shouldReprocessPost(post.ID, postEditTime, numComments)

		if postNeedsReprocessing {
			for _, keyword := range keywords {
				if containsKeyword(post.Title, keyword) || containsKeyword(post.Body, keyword) {
					fmt.Printf("Found keyword '%s' in post: %s\n", keyword, post.Title)
					batchPosts = append(batchPosts, post)
				}
			}
		}

		// Enqueue a job to fetch comments if needed
		if commentsNeedFetching && numComments > 0 {
			incrementActiveJobs(post.SubredditName)
			jobQueue <- Job{
				RequestType: "Comments",
				Post:        post,
				Keywords:    keywords,
			}
		}

		updatePostTracking(post.ID, subreddit, time.Now(), time.Now(), postEditTime, numComments)
	}

	if len(batchPosts) > 0 {
		saveToDatabase(batchPosts)
	}
	return nil
}

// Helper function to increment the job counter atomically
func incrementActiveJobs(subreddit string) bool {
	counter, _ := activeJobs.LoadOrStore(subreddit, new(int32))
	// Atomically increment and check if it's the first active job
	return atomic.AddInt32(counter.(*int32), 1) == 1
}

// Helper function to decrement the job counter atomically
func decrementActiveJobs(subreddit string) {
	if counter, exists := activeJobs.Load(subreddit); exists {
		// Atomically decrement and check if count reaches zero
		newCount := atomic.AddInt32(counter.(*int32), -1)
		if newCount <= 0 {
			activeJobs.Delete(subreddit) // Remove entry if no active jobs
		}
	}
}

// Process comments for a post
func processComments(post *reddit.Post, keywords []string, client *reddit.Client) error {
	thread, _, err := client.Post.Get(context.Background(), post.ID)
	if err != nil {
		return fmt.Errorf("error fetching comments: %w", err)
	}
	fmt.Printf("Fetched %d comments for post: %s\n", len(thread.Comments), post.Title)
	var batchComments []*reddit.Comment
	for _, comment := range thread.Comments {
		for _, keyword := range keywords {
			if containsKeyword(comment.Body, keyword) {
				fmt.Printf("Found keyword '%s' in comment: %s\n", keyword, comment.Body)
				batchComments = append(batchComments, comment)
			}
		}
	}

	if len(batchComments) > 0 {
		fmt.Printf("Saved %d comments to database\n", len(batchComments))
		saveCommentsToDatabase(batchComments)
	}

	return nil
}

func containsKeyword(text, keyword string) bool {
	return len(text) > 0 && (stringContains(text, keyword))
}

func stringContains(text, keyword string) bool {
	return strings.Contains(strings.ToLower(text), strings.ToLower(keyword))
}

func shouldReprocessPost(postID string, lastPostEdit time.Time, numComments int) (bool, bool) {
	var result TrackedPost
	filter := bson.M{"post_id": postID}
	err := trackedPostsCollection.FindOne(context.TODO(), filter).Decode(&result)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return true, true // Post is new, should be processed and fetch comments
	} else if err != nil {
		log.Println("Error querying MongoDB:", err)
		return false, false
	}

	postNeedsReprocessing := lastPostEdit.After(result.LastPostEdit)
	commentsNeedFetching := numComments > result.NumComments

	return postNeedsReprocessing, commentsNeedFetching
}

func updatePostTracking(postID, subreddit string, lastProcessed, lastCommentCheck, lastPostEdit time.Time, numComments int) {
	filter := bson.M{"post_id": postID}
	update := bson.M{
		"$set": TrackedPost{
			PostID:           postID,
			Subreddit:        subreddit,
			LastProcessed:    lastProcessed,
			LastCommentCheck: lastCommentCheck,
			LastPostEdit:     lastPostEdit,
			NumComments:      numComments,
		},
	}
	opts := options.Update().SetUpsert(true)
	_, err := trackedPostsCollection.UpdateOne(context.TODO(), filter, update, opts)
	if err != nil {
		log.Println("Error updating post tracking in MongoDB:", err)
	}
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

// / Batch database updates for posts and comments
func saveToDatabase(posts []*reddit.Post) {
	var postDocs []interface{}
	for _, post := range posts {
		postDoc := PostDocument{
			PostID:      post.ID,
			Title:       post.Title,
			Body:        post.Body,
			Subreddit:   post.SubredditName,
			CreatedAt:   post.Created.Time,
			UpdatedAt:   post.Edited.Time,
			NumComments: post.NumberOfComments,
		}
		postDocs = append(postDocs, postDoc)
	}

	if len(postDocs) > 0 {
		_, err := postsCollection.InsertMany(context.TODO(), postDocs)
		if err != nil {
			log.Println("Error saving posts to database:", err)
		} else {
			fmt.Println("Saved posts to database:", len(postDocs))
		}
	}
}

// Save a comment to MongoDB
func saveCommentsToDatabase(comments []*reddit.Comment) {
	var commentDocs []interface{}
	for _, comment := range comments {
		commentDoc := CommentDocument{
			CommentID: comment.ID,
			PostID:    comment.ParentID,
			Body:      comment.Body,
			Author:    comment.Author,
			CreatedAt: comment.Created.Time,
		}
		commentDocs = append(commentDocs, commentDoc)
	}

	if len(commentDocs) > 0 {
		_, err := commentsCollection.InsertMany(context.TODO(), commentDocs)
		if err != nil {
			log.Println("Error saving comments to database:", err)
		} else {
			fmt.Println("Saved comments to database:", len(commentDocs))
		}
	}
}
