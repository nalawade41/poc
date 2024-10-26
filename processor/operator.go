package processor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/nalawade41/poc/db/nosql"
	"github.com/nalawade41/poc/types"
	"github.com/vartanbeno/go-reddit/v2/reddit"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Operator struct {
	TrackedPostCollection *mongo.Collection
	PostsCollection       *mongo.Collection
	CommentsCollection    *mongo.Collection
}

func NewOperator(client *nosql.MongoDBClient) *Operator {
	return &Operator{
		TrackedPostCollection: client.GetCollection("tracked_posts"),
		PostsCollection:       client.GetCollection("posts"),
		CommentsCollection:    client.GetCollection("comments"),
	}
}

func (p *Operator) ShouldReprocessPost(ctx context.Context, postID string, lastPostEdit time.Time, numComments int) (bool, bool) {
	var result types.TrackedPost
	filter := bson.M{"post_id": postID}
	err := p.TrackedPostCollection.FindOne(ctx, filter).Decode(&result)
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

func (p *Operator) UpdatePostTracking(ctx context.Context, postID, subreddit string, lastProcessed, lastCommentCheck, lastPostEdit time.Time, numComments int) {
	filter := bson.M{"post_id": postID}
	update := bson.M{
		"$set": types.TrackedPost{
			PostID:           postID,
			Subreddit:        subreddit,
			LastProcessed:    lastProcessed,
			LastCommentCheck: lastCommentCheck,
			LastPostEdit:     lastPostEdit,
			NumComments:      numComments,
		},
	}
	opts := options.Update().SetUpsert(true)
	_, err := p.TrackedPostCollection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		log.Println("Error updating post tracking in MongoDB:", err)
	}
}

// SavePostsToDatabase Batch database updates for posts
func (p *Operator) SavePostsToDatabase(ctx context.Context, posts []*reddit.Post) {
	var postDocs []interface{}
	for _, post := range posts {
		postDoc := types.PostDocument{
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
		_, err := p.PostsCollection.InsertMany(ctx, postDocs)
		if err != nil {
			log.Println("Error saving posts to database:", err)
		} else {
			fmt.Println("Saved posts to database:", len(postDocs))
		}
	}
}

// SaveCommentsToDatabase a comment to MongoDB
func (p *Operator) SaveCommentsToDatabase(ctx context.Context, comments []*reddit.Comment) {
	var commentDocs []interface{}
	for _, comment := range comments {
		commentDoc := types.CommentDocument{
			CommentID: comment.ID,
			PostID:    comment.ParentID,
			Body:      comment.Body,
			Author:    comment.Author,
			CreatedAt: comment.Created.Time,
		}
		commentDocs = append(commentDocs, commentDoc)
	}

	if len(commentDocs) > 0 {
		_, err := p.CommentsCollection.InsertMany(ctx, commentDocs)
		if err != nil {
			log.Println("Error saving comments to database:", err)
		} else {
			fmt.Println("Saved comments to database:", len(commentDocs))
		}
	}
}
