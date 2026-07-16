package graphingest

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
)

// GraphFunc is the signature for a graph function.
// It receives a context (with GraphRunContext set) and returns a result or error.
type GraphFunc func(ctx context.Context) (any, error)

// GraphOpts configures a Graph wrapper.
type GraphOpts struct {
	// Name is the graph's unique key. Required.
	Name string
	// Version is an optional version string for the graph.
	Version string
	// Tags are optional metadata tags.
	Tags []string
	// TimeoutSeconds is the max execution time.
	// Defaults to tier limit (Free: 15min, Pro: 1hr, Enterprise: 6hr).
	// Clamped to tier max (Free: 30min, Pro: 6hr, Enterprise: 24hr).
	TimeoutSeconds int
	// RetryPolicy configures retry behavior. Zero value = no retries.
	RetryPolicy RetryPolicy
	// Retries is a simple retry count (legacy). Ignored if RetryPolicy.MaxRetries > 0.
	Retries int
	// RetryDelaySeconds is a simple fixed delay (legacy). Ignored if RetryPolicy is set.
	RetryDelaySeconds float64
	// OnCompletion is called when the graph completes successfully.
	OnCompletion func(ctx *GraphRunContextData, result any)
	// OnFailure is called when the graph fails after all retries.
	OnFailure func(ctx *GraphRunContextData, err error)
}

// GraphWrapper wraps a function with the GraphIngest graph lifecycle:
// parameter validation, context injection, timeout, retries with exponential backoff, and state hooks.
type GraphWrapper struct {
	opts GraphOpts
	fn   GraphFunc
}

// Graph creates a new GraphWrapper.
//
//	pipeline := graphingest.Graph(graphingest.GraphOpts{
//	    Name: "etl-pipeline",
//	    RetryPolicy: graphingest.RetryPolicy{MaxRetries: 3, DelaySeconds: 1, Jitter: true},
//	    TimeoutSeconds: 600,
//	}, func(ctx context.Context) (any, error) {
//	    // orchestrate nodes here
//	    return result, nil
//	})
func Graph(opts GraphOpts, fn GraphFunc) *GraphWrapper {
	if opts.Name == "" {
		panic("graphingest: GraphOpts.Name is required")
	}
	return &GraphWrapper{opts: opts, fn: fn}
}

// Run executes the graph with full lifecycle management:
// context injection, timeout, retry loop, logging, and state hooks.
func (g *GraphWrapper) Run(ctx context.Context, parameters map[string]any) (any, error) {
	// Resolve retry policy (prefer RetryPolicy, fall back to legacy Retries/RetryDelaySeconds)
	rp := g.opts.RetryPolicy
	if rp.MaxRetries == 0 && g.opts.Retries > 0 {
		rp.MaxRetries = g.opts.Retries
		rp.DelaySeconds = g.opts.RetryDelaySeconds
		if rp.BackoffFactor == 0 {
			rp.BackoffFactor = 1.0 // fixed delay for legacy
		}
		rp.Jitter = false
	}

	// Detect parent graph context for subgraph nesting
	parentCtx := GraphRunContextFrom(ctx)
	var graphRunID string
	var parentGraphRunID string

	if parentCtx != nil {
		// Subgraph: generate a new run ID, link to parent
		graphRunID = uuid.New().String()
		parentGraphRunID = parentCtx.GraphRunID
		log.Printf("[graphingest] Subgraph %q spawned from parent %q", g.opts.Name, parentCtx.GraphName)
	} else {
		graphRunID = envOrDefault("GRAPH_RUN_ID", envOrDefault("FLOW_RUN_ID", uuid.New().String()))
	}

	graphCtx := &GraphRunContextData{
		GraphRunID:       graphRunID,
		GraphName:        g.opts.Name,
		GraphVersion:     g.opts.Version,
		Parameters:       parameters,
		Tags:             g.opts.Tags,
		ParentGraphRunID: parentGraphRunID,
	}
	ctx = WithGraphRunContext(ctx, graphCtx)

	// Apply timeout (always enforced via tier limits)
	limits := getLimits()
	graphTimeout := clampTimeout(g.opts.TimeoutSeconds, limits.GraphDefaultTimeout, limits.GraphMaxTimeout)
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, time.Duration(graphTimeout)*time.Second)
	defer cancel()

	// Set up streaming logger
	logger := NewStreamingLogger(StreamingLoggerOpts{
		FlowRunID: graphRunID,
	})
	defer logger.Close()

	logger.Infof("Starting graph: %s (run=%s)", g.opts.Name, graphRunID)
	start := time.Now()

	// Retry loop
	maxAttempts := rp.MaxRetries + 1
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			delay := rp.ComputeDelay(attempt - 1)
			logger.Warnf("Graph %s: retry %d/%d after %v", g.opts.Name, attempt, rp.MaxRetries, delay)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				lastErr = fmt.Errorf("graphingest: graph %q timed out during retry delay: %w", g.opts.Name, ctx.Err())
				break
			}
		}

		result, err := g.fn(ctx)
		if err == nil {
			duration := time.Since(start)
			logger.Infof("Graph %s completed in %v", g.opts.Name, duration)
			if g.opts.OnCompletion != nil {
				g.opts.OnCompletion(graphCtx, result)
			}
			return result, nil
		}

		lastErr = err
		logger.Errorf("Graph %s attempt %d failed: %v", g.opts.Name, attempt+1, err)

		// Don't retry on context cancellation/timeout
		if ctx.Err() != nil {
			break
		}
	}

	duration := time.Since(start)
	logger.Errorf("Graph %s failed after %v (%d attempts): %v", g.opts.Name, duration, maxAttempts, lastErr)

	if g.opts.OnFailure != nil {
		g.opts.OnFailure(graphCtx, lastErr)
	}

	return nil, lastErr
}

// GraphTimeoutError is returned when a graph exceeds its timeout.
type GraphTimeoutError struct {
	GraphName string
	Timeout   time.Duration
}

func (e *GraphTimeoutError) Error() string {
	return fmt.Sprintf("graphingest: graph %q timed out after %v", e.GraphName, e.Timeout)
}

func init() {
	// Seed the worker ID if not set
	if os.Getenv("WORKER_ID") == "" {
		_ = os.Setenv("WORKER_ID", "go-sdk")
	}
}
