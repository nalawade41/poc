package main

import (
	"context"
	"log"

	"github.com/nalawade41/poc/db/nosql"
	"github.com/nalawade41/poc/processor"
	"github.com/nalawade41/poc/scheduler"
	"github.com/nalawade41/poc/types"
	"github.com/vartanbeno/go-reddit/v2/reddit"
)

func main() {
	// Example subreddits to monitor
	subreddits := []string{"golang", "programming"}
	keywords := []string{"Goroutine", "Channel", "Concurrency"}

	// Create context to allow graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Assign cancel function to global variable for debugging purposes
	//constants.CancelFunc = cancel

	// Create a MongoDB client
	mongoClient := nosql.NewMongoDBClient("reddit_tracker")

	// Connect to MongoDB
	if err := mongoClient.InitMongoClient(); err != nil {
		log.Fatal(err)
	}

	defer func() { mongoClient.DisconnectMongoClient() }()

	if !mongoClient.VerifyMongoClient(ctx) {
		log.Fatal("Failed to connect to MongoDB")
	}

	// Create a Reddit client
	client := reddit.DefaultClient()

	//TODO: See if we can move this to the other common package
	// Create a job queue and start the worker pool
	jobQueue := make(chan types.Job, 100) // Buffer size of 100 jobs

	// Start the scheduler with subreddit monitoring
	scheduler.Start(ctx, jobQueue, subreddits, keywords, processor.NewProcessorFactory(client, jobQueue, processor.NewOperator(mongoClient)))
}
