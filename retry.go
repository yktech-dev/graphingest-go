package graphingest

import (
	"math"
	"math/rand"
	"time"
)

// RetryPolicy configures retry behavior with exponential backoff and optional jitter.
//
// Delay formula:
//
//	delay = min(DelaySeconds * BackoffFactor^attempt, MaxDelaySeconds)
//	if Jitter: delay = delay * rand(0.5, 1.5)
type RetryPolicy struct {
	// MaxRetries is the total number of retry attempts (0 = no retries).
	MaxRetries int

	// DelaySeconds is the initial delay between retries.
	DelaySeconds float64

	// BackoffFactor is the multiplier applied after each attempt (default 2.0).
	BackoffFactor float64

	// MaxDelaySeconds is the upper bound on computed delay (default 120.0).
	MaxDelaySeconds float64

	// Jitter randomizes the delay ±50% to avoid thundering herd (default true).
	Jitter bool
}

// DefaultRetryPolicy returns a RetryPolicy with sensible defaults.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxRetries:      0,
		DelaySeconds:    0,
		BackoffFactor:   2.0,
		MaxDelaySeconds: 120.0,
		Jitter:          true,
	}
}

// ComputeDelay calculates the delay for a given attempt number (0-indexed).
func (rp RetryPolicy) ComputeDelay(attempt int) time.Duration {
	if rp.DelaySeconds <= 0 {
		return 0
	}

	factor := rp.BackoffFactor
	if factor <= 0 {
		factor = 2.0
	}
	maxDelay := rp.MaxDelaySeconds
	if maxDelay <= 0 {
		maxDelay = 120.0
	}

	delay := rp.DelaySeconds * math.Pow(factor, float64(attempt))
	delay = math.Min(delay, maxDelay)

	if rp.Jitter {
		// ±50% jitter
		jitterFactor := 0.5 + rand.Float64() // [0.5, 1.5)
		delay *= jitterFactor
	}

	return time.Duration(delay * float64(time.Second))
}
