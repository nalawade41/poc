package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nalawade41/poc/constants"
	"github.com/nalawade41/poc/processor"
	"github.com/nalawade41/poc/types"
	"github.com/nalawade41/poc/util"
)

var rateLimiter = time.Tick(time.Minute / constants.MaxRequestsPerMinute)

// Start initializes the worker pool, job queue, and scheduling for subreddit monitoring.
func Start(ctx context.Context, jobQueue chan types.Job, subreddits []string, keywords []string, processorFactory *processor.ProcessorFactory) {
	var wg sync.WaitGroup

	// Initialize the worker pool
	for i := 0; i < constants.WorkerCount; i++ {
		wg.Add(1)
		go Worker(ctx, jobQueue, &wg, processorFactory)
	}

	// Schedule fetching new posts for each subreddit
	for _, subreddit := range subreddits {
		go ScheduleNewPostsFetching(ctx, jobQueue, subreddit, keywords)
	}

	// Start the counter logging goroutine
	go util.LogRequestRate()

	// Wait for all workers to finish
	wg.Wait()
}

// Worker function to process jobs from the job queue
func Worker(ctx context.Context, jobQueue chan types.Job, wg *sync.WaitGroup, processorFactory *processor.ProcessorFactory) {
	defer wg.Done()
	for job := range jobQueue {
		// Global rate limiter to control request rate
		<-rateLimiter

		// Increment the request counter
		util.AddRequestCounter()

		var err error
		switch job.RequestType {
		case "NewPosts":
			err = processorFactory.GetPostProcessor().ProcessPosts(ctx, job.Subreddit, job.Keywords)
			if err == nil {
				util.DecrementActiveJobs(job.Subreddit)
			} else {
				HandleRetry(job, err, jobQueue)
			}
		case "Comments":
			err = processorFactory.GetCommentsProcessor().ProcessComments(ctx, job.Post, job.Keywords)
			if err == nil {
				util.DecrementActiveJobs(job.Post.SubredditName)
			} else {
				HandleRetry(job, err, jobQueue)
			}
		}
	}
}

// HandleRetry is a helper function to handle retries
func HandleRetry(job types.Job, err error, jobQueue chan types.Job) {
	job.RetryCount++
	if job.RetryCount >= constants.MaxRetries {
		fmt.Printf("Max retries reached for %s job in subreddit %s. Skipping...\n", job.RequestType, job.Subreddit)
		if job.RequestType == "NewPosts" {
			util.DecrementActiveJobs(job.Subreddit)
		} else if job.RequestType == "Comments" {
			util.DecrementActiveJobs(job.Post.SubredditName)
		}
	} else {
		fmt.Printf("Error processing %s job for subreddit %s: %v. Retrying (%d/%d)...\n",
			job.RequestType, job.Subreddit, err, job.RetryCount, constants.MaxRetries)
		jobQueue <- job // Requeue job on failure for retry
	}
}

// ScheduleNewPostsFetching schedules fetching new posts for a subreddit at regular intervals
func ScheduleNewPostsFetching(ctx context.Context, jobQueue chan types.Job, subreddit string, keywords []string) {
	// Run the job once initially
	if util.IncrementActiveJobs(subreddit) {
		jobQueue <- types.Job{
			RequestType: "NewPosts",
			Subreddit:   subreddit,
			Keywords:    keywords,
		}
	}

	ticker := time.NewTicker(constants.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !util.IncrementActiveJobs(subreddit) {
				continue // Skip if there's already an active job for this subreddit
			}
			fmt.Printf("Scheduled job for subreddit: %s\n", subreddit)
			// Enqueue a NewPosts job for this subreddit
			jobQueue <- types.Job{
				RequestType: "NewPosts",
				Subreddit:   subreddit,
				Keywords:    keywords,
			}
		}
	}
}
