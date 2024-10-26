package util

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/nalawade41/poc/constants"
)

// IncrementActiveJobs function to increment the job counter atomically
func IncrementActiveJobs(subreddit string) bool {
	counter, _ := constants.ActiveJobs.LoadOrStore(subreddit, new(int32))
	// Atomically increment and check if it's the first active job
	return atomic.AddInt32(counter.(*int32), 1) == 1
}

// DecrementActiveJobs Helper function to decrement the job counter atomically
func DecrementActiveJobs(subreddit string) {
	if counter, exists := constants.ActiveJobs.Load(subreddit); exists {
		// Atomically decrement and check if count reaches zero
		newCount := atomic.AddInt32(counter.(*int32), -1)
		if newCount <= 0 {
			fmt.Printf("Finished subreddit: %s\n", subreddit)
			constants.ActiveJobs.Delete(subreddit) // Remove entry if no active jobs
		}
	}
}

// FirstRequestRateLog logs the number of requests made each minute and resets the counter.
func FirstRequestRateLog(cancelFunc context.CancelFunc) {
	// Start a timer only on the first request
	constants.FirstRequestOnce.Do(func() {
		fmt.Println("First request hit. Starting 1-minute timer.")
		go func() {
			time.Sleep(1 * time.Minute)
			fmt.Println("1 minute passed since the first request. Stopping execution.")
			cancelFunc()
			// Cancel context to stop execution
		}()
	})
}

func AddRequestCounter() {
	atomic.AddInt32(&constants.RequestRateCounter, 1)
}
