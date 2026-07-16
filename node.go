package graphingest

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// Platform limits (tier-based)
// ---------------------------------------------------------------------------

// TierLimits defines timeout limits for a platform tier.
type TierLimits struct {
	NodeDefaultTimeout      int // seconds
	NodeMaxTimeout          int // seconds
	GraphDefaultTimeout     int // seconds
	GraphMaxTimeout         int // seconds
	MonthlyExecutionMinutes int // 0 = unlimited
	MaxPipelines            int // 0 = unlimited
}

// PlatformLimits maps tier names to their timeout limits.
var PlatformLimits = map[string]TierLimits{
	"free": {
		NodeDefaultTimeout:       300,   // 5 min
		NodeMaxTimeout:           600,   // 10 min
		GraphDefaultTimeout:      900,   // 15 min
		GraphMaxTimeout:          1800,  // 30 min
		MonthlyExecutionMinutes:  60,    // 60 min/mo total
		MaxPipelines:             5,
	},
	"pro": {
		NodeDefaultTimeout:       600,   // 10 min
		NodeMaxTimeout:           3600,  // 60 min
		GraphDefaultTimeout:      3600,  // 1 hr
		GraphMaxTimeout:          21600, // 6 hr
		MonthlyExecutionMinutes:  0,     // unlimited
		MaxPipelines:             0,     // unlimited
	},
	"enterprise": {
		NodeDefaultTimeout:       3600,  // 60 min
		NodeMaxTimeout:           86400, // 24 hr
		GraphDefaultTimeout:      21600, // 6 hr
		GraphMaxTimeout:          86400, // 24 hr
		MonthlyExecutionMinutes:  0,     // unlimited
		MaxPipelines:             0,     // unlimited
	},
}

func getTier() string {
	if t := os.Getenv("GRAPHINGEST_TIER"); t != "" {
		return t
	}
	return "free"
}

func getLimits() TierLimits {
	l, ok := PlatformLimits[getTier()]
	if !ok {
		return PlatformLimits["free"]
	}
	return l
}

func clampTimeout(requested, defaultVal, maxVal int) int {
	if requested <= 0 {
		return defaultVal
	}
	if requested > maxVal {
		log.Printf("[graphingest] WARNING: Requested timeout %ds exceeds %s tier max (%ds). Clamped.", requested, getTier(), maxVal)
		return maxVal
	}
	return requested
}

// NodeTimeoutError is returned when a node exceeds its timeout.
type NodeTimeoutError struct {
	NodeName string
	Timeout  time.Duration
}

func (e *NodeTimeoutError) Error() string {
	return fmt.Sprintf("graphingest: node %q timed out after %v", e.NodeName, e.Timeout)
}

// ---------------------------------------------------------------------------
// Node
// ---------------------------------------------------------------------------

// NodeFunc is the signature for a node function.
// It receives a context (with NodeRunContext set) and input, and returns a result or error.
type NodeFunc[In any, Out any] func(ctx context.Context, input In) (Out, error)

// NodeOpts configures a Node wrapper.
type NodeOpts struct {
	// Name is the node's unique key. Required.
	Name string
	// CacheTTL is the duration (in seconds) to cache results based on input hash. 0 = no caching.
	CacheTTL int
	// TimeoutSeconds is the max execution time per node run.
	// Defaults to tier limit (Free: 5min, Pro: 10min, Enterprise: 60min).
	// Clamped to tier max (Free: 10min, Pro: 60min, Enterprise: 24hr).
	TimeoutSeconds int
}

// NodeWrapper wraps a function with the GraphIngest node lifecycle.
// It provides Map() for parallel fan-out and Submit() for async dispatch.
type NodeWrapper[In any, Out any] struct {
	opts NodeOpts
	fn   NodeFunc[In, Out]
}

// Node creates a new NodeWrapper around the given function.
//
//	extract := graphingest.Node(graphingest.NodeOpts{Name: "extract"}, func(ctx context.Context, url string) (map[string]any, error) {
//	    return map[string]any{"url": url, "rows": 100}, nil
//	})
func Node[In any, Out any](opts NodeOpts, fn NodeFunc[In, Out]) *NodeWrapper[In, Out] {
	if opts.Name == "" {
		panic("graphingest: NodeOpts.Name is required")
	}
	return &NodeWrapper[In, Out]{opts: opts, fn: fn}
}

// Run executes the node function with full lifecycle management:
// context injection, logging, result reporting, and caching.
func (n *NodeWrapper[In, Out]) Run(ctx context.Context, input In) (Out, error) {
	graphCtx := GraphRunContextFrom(ctx)
	graphRunID := ""
	if graphCtx != nil {
		graphRunID = graphCtx.GraphRunID
	} else {
		graphRunID = envOrDefault("GRAPH_RUN_ID", envOrDefault("FLOW_RUN_ID", ""))
	}

	nodeRunID := envOrDefault("NODE_RUN_ID", envOrDefault("TASK_RUN_ID", uuid.New().String()))

	// Set NodeRunContext
	nodeCtx := &NodeRunContextData{
		NodeRunID:  nodeRunID,
		NodeKey:    n.opts.Name,
		GraphRunID: graphRunID,
	}
	ctx = WithNodeRunContext(ctx, nodeCtx)

	// Apply node timeout
	limits := getLimits()
	nodeTimeout := clampTimeout(n.opts.TimeoutSeconds, limits.NodeDefaultTimeout, limits.NodeMaxTimeout)
	timeoutDur := time.Duration(nodeTimeout) * time.Second
	ctx, cancel := context.WithTimeout(ctx, timeoutDur)
	defer cancel()

	log.Printf("[graphingest] Starting node: %s (run=%s, timeout=%ds)", n.opts.Name, nodeRunID, nodeTimeout)
	start := time.Now()

	result, err := n.fn(ctx, input)
	duration := time.Since(start)

	if ctx.Err() == context.DeadlineExceeded {
		log.Printf("[graphingest] Node %s timed out after %v", n.opts.Name, timeoutDur)
		var zero Out
		return zero, &NodeTimeoutError{NodeName: n.opts.Name, Timeout: timeoutDur}
	}

	if err != nil {
		log.Printf("[graphingest] Node %s failed after %v: %v", n.opts.Name, duration, err)
		return result, err
	}

	log.Printf("[graphingest] Node %s completed in %v", n.opts.Name, duration)
	return result, nil
}

// Map dispatches the node across multiple inputs in parallel via Cloud Run fan-out.
// All inputs are dispatched concurrently; results are collected in input order.
// Must be called from within a Graph function (requires GraphRunContext in ctx).
func (n *NodeWrapper[In, Out]) Map(ctx context.Context, items []In, opts *MapOpts) ([]Out, error) {
	graphCtx := GraphRunContextFrom(ctx)
	if graphCtx == nil {
		return nil, fmt.Errorf("graphingest: .Map() must be called from within a Graph function")
	}

	// Convert typed inputs to []any for the API
	inputs := make([]any, len(items))
	for i, item := range items {
		inputs[i] = item
	}

	client := GetClient()
	dispatch, err := client.DispatchNodes(graphCtx.GraphRunID, n.opts.Name, inputs)
	if err != nil {
		return nil, fmt.Errorf("graphingest: dispatch .Map() for node %q: %w", n.opts.Name, err)
	}

	log.Printf("[graphingest] Mapped %d invocations of node %q", len(dispatch.TaskRunIDs), n.opts.Name)

	pollInterval := 1 * time.Second
	var timeout time.Duration
	if opts != nil {
		if opts.PollInterval > 0 {
			pollInterval = opts.PollInterval
		}
		timeout = opts.Timeout
	}

	start := time.Now()
	var status *PollTaskRunsResult
	for {
		status, err = client.PollTaskRuns(dispatch.TaskRunIDs)
		if err != nil {
			return nil, fmt.Errorf("graphingest: poll .Map() results: %w", err)
		}
		if status.AllCompleted {
			break
		}
		if timeout > 0 && time.Since(start) > timeout {
			return nil, fmt.Errorf("graphingest: .Map() timed out after %v waiting for node %q", timeout, n.opts.Name)
		}
		time.Sleep(pollInterval)
	}

	// Collect results in dispatch order
	byID := make(map[string]TaskRunStatus, len(status.Results))
	for _, r := range status.Results {
		byID[r.ID] = r
	}

	results := make([]Out, len(dispatch.TaskRunIDs))
	for i, id := range dispatch.TaskRunIDs {
		r, ok := byID[id]
		if !ok {
			return nil, fmt.Errorf("graphingest: missing result for task run %s", id)
		}
		if r.State == "FAILED" {
			return nil, fmt.Errorf("graphingest: node %q (mapped index %d) failed: %s", n.opts.Name, i, r.ErrorMessage)
		}
		// Unmarshal the result data into Out type
		data, marshalErr := json.Marshal(r.ResultData)
		if marshalErr != nil {
			return nil, fmt.Errorf("graphingest: marshal result for task %s: %w", id, marshalErr)
		}
		if err := json.Unmarshal(data, &results[i]); err != nil {
			return nil, fmt.Errorf("graphingest: unmarshal result for task %s: %w", id, err)
		}
	}

	return results, nil
}

// Submit dispatches a single node execution asynchronously, returning a NodeFuture.
// Must be called from within a Graph function (requires GraphRunContext in ctx).
func (n *NodeWrapper[In, Out]) Submit(ctx context.Context, input In) (*NodeFuture, error) {
	graphCtx := GraphRunContextFrom(ctx)
	if graphCtx == nil {
		return nil, fmt.Errorf("graphingest: .Submit() must be called from within a Graph function")
	}

	client := GetClient()
	dispatch, err := client.DispatchNodes(graphCtx.GraphRunID, n.opts.Name, []any{input})
	if err != nil {
		return nil, fmt.Errorf("graphingest: dispatch .Submit() for node %q: %w", n.opts.Name, err)
	}

	if len(dispatch.TaskRunIDs) == 0 {
		return nil, fmt.Errorf("graphingest: .Submit() returned no task run IDs")
	}

	taskRunID := dispatch.TaskRunIDs[0]
	log.Printf("[graphingest] Submitted node %q → %s", n.opts.Name, taskRunID)
	return NewNodeFuture(taskRunID, n.opts.Name, client), nil
}

// MapOpts configures fan-out behavior.
type MapOpts struct {
	// PollInterval is the delay between status polls (default 1s).
	PollInterval time.Duration
	// Timeout is the maximum time to wait for all results. Zero means no timeout.
	Timeout time.Duration
}

// ComputeInputHash returns a SHA-256 hash of the node key + input data for caching.
func ComputeInputHash(nodeKey string, inputData any) string {
	payload, _ := json.Marshal(map[string]any{"taskKey": nodeKey, "inputData": inputData})
	h := sha256.Sum256(payload)
	return fmt.Sprintf("%x", h)
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
