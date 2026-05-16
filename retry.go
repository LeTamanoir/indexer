package indexer

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/rpc"
)

// RetryPolicy controls retry behavior. MaxAttempts is the total number of
// attempts; values less than or equal to zero retry until the context ends.
type RetryPolicy struct {
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	MaxAttempts    int
	Retryable      func(error) bool
}

// DefaultRetryPolicy retries common transient RPC and network failures.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		InitialBackoff: 500 * time.Millisecond,
		MaxBackoff:     10 * time.Second,
		MaxAttempts:    5,
		Retryable:      DefaultRetryable,
	}
}

// DefaultRetryable reports whether err is a transient error worth retrying.
func DefaultRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var httpErr rpc.HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode == 429 || httpErr.StatusCode == 500 || httpErr.StatusCode == 502 || httpErr.StatusCode == 503 || httpErr.StatusCode == 504
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}

	message := strings.ToLower(err.Error())
	return strings.Contains(message, "rate limit") ||
		strings.Contains(message, "too many requests") ||
		strings.Contains(message, "temporarily unavailable") ||
		strings.Contains(message, "timeout") ||
		strings.Contains(message, "connection reset")
}
