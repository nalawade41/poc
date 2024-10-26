package constants

import (
	"context"
	"sync"
	"time"
)

const (
	MaxRequestsPerMinute = 60
	WorkerCount          = 5                 // Number of workers in the pool
	PollInterval         = 600 * time.Second // Adjust the polling interval as necessary
	MaxRetries           = 3                 // Maximum retry attempts for failed jobs
)

var (
	RequestRateCounter int32
	ActiveJobs         sync.Map
	FirstRequestOnce   sync.Once
	CancelFunc         context.CancelFunc
)
