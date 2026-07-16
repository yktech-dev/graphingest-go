package graphingest

import (
	"fmt"
	"time"
)

// NodeFuture represents a handle to an asynchronously dispatched node execution.
// Use Result() to block until the node completes and retrieve its output.
type NodeFuture struct {
	taskRunID string
	nodeKey   string
	client    *GraphIngestClient
	result    any
	err       error
	resolved  bool
}

// NewNodeFuture creates a new NodeFuture for tracking an async node dispatch.
func NewNodeFuture(taskRunID, nodeKey string, client *GraphIngestClient) *NodeFuture {
	return &NodeFuture{
		taskRunID: taskRunID,
		nodeKey:   nodeKey,
		client:    client,
	}
}

// TaskRunID returns the ID of the dispatched task run.
func (f *NodeFuture) TaskRunID() string {
	return f.taskRunID
}

// NodeKey returns the node key this future represents.
func (f *NodeFuture) NodeKey() string {
	return f.nodeKey
}

// ResultOpts configures how Result() polls for completion.
type ResultOpts struct {
	// Timeout is the maximum time to wait for the result. Zero means no timeout.
	Timeout time.Duration
	// PollInterval is the delay between status checks (default 1s).
	PollInterval time.Duration
}

// Result blocks until the node completes and returns its result.
// Returns an error if the node failed or the timeout was reached.
func (f *NodeFuture) Result(opts *ResultOpts) (any, error) {
	if f.resolved {
		return f.result, f.err
	}

	pollInterval := 1 * time.Second
	var timeout time.Duration
	if opts != nil {
		if opts.PollInterval > 0 {
			pollInterval = opts.PollInterval
		}
		timeout = opts.Timeout
	}

	start := time.Now()
	for {
		status, err := f.client.PollTaskRuns([]string{f.taskRunID})
		if err != nil {
			return nil, fmt.Errorf("graphingest: poll task run %s: %w", f.taskRunID, err)
		}

		if len(status.Results) > 0 {
			r := status.Results[0]
			switch r.State {
			case "COMPLETED":
				f.result = r.ResultData
				f.resolved = true
				return f.result, nil
			case "FAILED":
				f.err = fmt.Errorf("graphingest: node %q (run %s) failed: %s", f.nodeKey, f.taskRunID, r.ErrorMessage)
				f.resolved = true
				return nil, f.err
			}
		}

		if timeout > 0 && time.Since(start) > timeout {
			return nil, fmt.Errorf("graphingest: timeout waiting for node %q (run %s) after %v", f.nodeKey, f.taskRunID, timeout)
		}

		time.Sleep(pollInterval)
	}
}
