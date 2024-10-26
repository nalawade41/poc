package util

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/nalawade41/poc/constants"
)

// LogRequestRate logs the number of requests made each minute and resets the counter.
func LogRequestRate() {
	for range time.Tick(time.Minute) {
		count := atomic.SwapInt32(&constants.RequestRateCounter, 0) // Reset counter atomically
		log.Printf("Total requests to Reddit in the last minute: %d", count)

		// TODO: Save this to better place cause this will give us idea about the rate of requests made to Reddit
		// So we can use this data to optimize the rate of requests made to Reddit
	}
}
