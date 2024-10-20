package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/vartanbeno/go-reddit/v2/reddit"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const maxRequestsPerMinute = 80
const pollInterval = 30 * time.Second // Adjust the polling interval as necessary

// Rate limiter to enforce the API rate limit
var rateLimiter = time.Tick(time.Minute / maxRequestsPerMinute)

var mongoClient *mongo.Client
var trackedPostsCollection *mongo.Collection

// TrackedPost MongoDB schema: tracked_posts
type TrackedPost struct {
	PostID           string    `bson:"post_id"`
	Subreddit        string    `bson:"subreddit"`
	LastProcessed    time.Time `bson:"last_processed"`
	LastCommentCheck time.Time `bson:"last_comment_check"`
	LastPostEdit     time.Time `bson:"last_post_edit"`
	NumComments      int       `bson:"num_comments"` // Track the number of comments
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

	// Example subreddit to monitor
	subreddits := []string{"golang", "programming"}
	keywords := []string{"Goroutine", "Channel", "Concurrency"}

	// Create context to allow graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Monitor subreddits
	for _, subreddit := range subreddits {
		go monitorSubreddit(ctx, subreddit, keywords)
	}

	// Wait for a signal to stop (for demo, we run indefinitely)
	select {}
}

// Check if post should be reprocessed
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

	// Reprocess the post if edited, fetch comments only if comment count increased
	postNeedsReprocessing := lastPostEdit.After(result.LastPostEdit)
	commentsNeedFetching := numComments > result.NumComments

	return postNeedsReprocessing, commentsNeedFetching
}

// Update post tracking data in MongoDB
func updatePostTracking(postID, subreddit string, lastProcessed, lastCommentCheck, lastPostEdit time.Time, numComments int) {
	filter := bson.M{"post_id": postID}
	update := bson.M{
		"$set": TrackedPost{
			PostID:           postID,
			Subreddit:        subreddit,
			LastProcessed:    lastProcessed,
			LastCommentCheck: lastCommentCheck,
			LastPostEdit:     lastPostEdit,
			NumComments:      numComments, // Update the comment count
		},
	}

	opts := options.Update().SetUpsert(true) // Use upsert to insert if not exists
	_, err := trackedPostsCollection.UpdateOne(context.TODO(), filter, update, opts)
	if err != nil {
		log.Println("Error updating post tracking in MongoDB:", err)
	}
}

// Monitor a specific subreddit
func monitorSubreddit(ctx context.Context, subreddit string, keywords []string) {
	//TODO: Need to figure out how to authenticate with Reddit API
	//client, err := reddit.NewClient(reddit.Credentials{
	//	ID:       "73gn7TG1Skgbyl8Ys9-kfA",
	//	Secret:   "5DhsgQ6IT0Oh1B4MIHOWvTcGtOMR6A",
	//	Username: "Impossible-Fun7405",
	//	Password: "abcABC1!",
	//})
	//if err != nil {
	//	log.Fatal(err)
	//}

	// For current use-case we will use the default client
	client := reddit.DefaultClient()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Stopping monitoring for subreddit:", subreddit)
			return
		case <-rateLimiter: // Enforce rate limit
			posts, _, err := client.Subreddit.NewPosts(ctx, "golang", &reddit.ListOptions{
				Limit: 100,
			})
			if err != nil {
				log.Println("Error fetching posts:", err)
				continue
			}

			// Process each post
			for _, post := range posts {
				// Get post-edit time and comment count
				postEditTime := post.Edited.Time
				numComments := post.NumberOfComments

				// Check if the post or comments need reprocessing
				postNeedsReprocessing, commentsNeedFetching := shouldReprocessPost(post.ID, postEditTime, numComments)

				if postNeedsReprocessing {
					for _, keyword := range keywords {
						if containsKeyword(post.Title, keyword) || containsKeyword(post.Body, keyword) {
							fmt.Printf("Found keyword '%s' in post: %s\n", keyword, post.Title)
							saveToDatabase(post)
						}
					}

					// Fetch comments only if the comment count has increased
					if commentsNeedFetching {
						comments, err := fetchComments(ctx, client, post)
						if err != nil {
							log.Println("Error fetching comments:", err)
							continue
						}

						// Process comments
						for _, comment := range comments {
							for _, keyword := range keywords {
								if containsKeyword(comment.Body, keyword) {
									fmt.Printf("Found keyword '%s' in comment: %s\n", keyword, comment.Body)
									saveCommentToDatabase(comment)
								}
							}
						}
					}

					// Update tracking data for this post in MongoDB
					updatePostTracking(post.ID, subreddit, time.Now(), time.Now(), postEditTime, numComments)
				}
			}

			time.Sleep(pollInterval)
		}
	}
}

func fetchComments(ctx context.Context, client *reddit.Client, post *reddit.Post) ([]*reddit.Comment, error) {
	<-rateLimiter // Enforce rate limit

	thread, _, err := client.Post.Get(ctx, post.ID)
	if err != nil {
		return nil, err
	}

	return thread.Comments, nil
}

func containsKeyword(text, keyword string) bool {
	return len(text) > 0 && (stringContains(text, keyword))
}

func stringContains(text, keyword string) bool {
	// Case-insensitive comparison
	return strings.Contains(strings.ToLower(text), strings.ToLower(keyword))
}

func saveToDatabase(post *reddit.Post) {
	// Implement your logic to save the post-data to the database here
	fmt.Println("Saving post to database:", post.Title)
}

func saveCommentToDatabase(comment *reddit.Comment) {
	// Implement your logic to save the comment data to the database here
	fmt.Println("Saving comment to database:", comment.Body)
}
