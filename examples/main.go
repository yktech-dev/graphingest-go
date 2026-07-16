// GraphIngest Go SDK — Feature Demo
//
// Demonstrates all SDK capabilities:
//   1. Node wrapper with typed input/output
//   2. Graph wrapper with retry policy (exponential backoff + jitter)
//   3. .Map() fan-out (parallel Cloud Run dispatch)
//   4. .Submit() async dispatch with NodeFuture
//   5. Subgraphs (nested Graph inside Graph)
//
// Prerequisites:
//
//	export GRAPHINGEST_API_URL=http://localhost:3000
//	export GRAPHINGEST_API_KEY=your-key
//	go run ./examples/
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	gi "github.com/yktech-dev/graphingest-go"
)

// ---------------------------------------------------------------------------
// 1. Define Node functions
// ---------------------------------------------------------------------------

var extract = gi.Node(gi.NodeOpts{Name: "extract"}, func(ctx context.Context, url string) (map[string]any, error) {
	log.Printf("  [extract] Fetching %s", url)
	// Simulate fetching data
	return map[string]any{"url": url, "rows": 100}, nil
})

var transform = gi.Node(gi.NodeOpts{Name: "transform"}, func(ctx context.Context, data map[string]any) (map[string]any, error) {
	log.Printf("  [transform] Cleaning data from %v", data["url"])
	data["cleaned"] = true
	return data, nil
})

var load = gi.Node(gi.NodeOpts{Name: "load"}, func(ctx context.Context, data map[string]any) (string, error) {
	log.Printf("  [load] Loading %v rows", data["rows"])
	return fmt.Sprintf("loaded %v rows from %v", data["rows"], data["url"]), nil
})

// ---------------------------------------------------------------------------
// 2. Define a Graph with RetryPolicy
// ---------------------------------------------------------------------------

var pipeline = gi.Graph(gi.GraphOpts{
	Name:    "etl-pipeline",
	Version: "1.0",
	Tags:    []string{"demo", "etl"},
	RetryPolicy: gi.RetryPolicy{
		MaxRetries:      3,
		DelaySeconds:    1.0,
		BackoffFactor:   2.0,
		MaxDelaySeconds: 30.0,
		Jitter:          true,
	},
	TimeoutSeconds: 300,
	OnCompletion: func(ctx *gi.GraphRunContextData, result any) {
		log.Printf("  ✓ Pipeline completed: %v", result)
	},
	OnFailure: func(ctx *gi.GraphRunContextData, err error) {
		log.Printf("  ✗ Pipeline failed: %v", err)
	},
}, func(ctx context.Context) (any, error) {
	// Access graph context
	gCtx := gi.GraphRunContextFromOrPanic(ctx)
	log.Printf("  Graph: %s (run=%s)", gCtx.GraphName, gCtx.GraphRunID)

	urls := []string{
		"https://api.example.com/a",
		"https://api.example.com/b",
		"https://api.example.com/c",
	}

	// Fan-out: run extract on all URLs in parallel
	results, err := extract.Map(ctx, urls, &gi.MapOpts{
		PollInterval: 500 * time.Millisecond,
		Timeout:      60 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("extract.Map failed: %w", err)
	}
	log.Printf("  Extracted %d results", len(results))

	// Async dispatch: submit transform and get a future
	future, err := transform.Submit(ctx, results[0])
	if err != nil {
		return nil, fmt.Errorf("transform.Submit failed: %w", err)
	}
	log.Printf("  Submitted transform → future %s", future.TaskRunID())

	// Block on future result
	transformResult, err := future.Result(&gi.ResultOpts{Timeout: 30 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("transform future failed: %w", err)
	}
	log.Printf("  Transform result: %v", transformResult)

	// Run subgraph
	subResult, err := subPipeline.Run(ctx, map[string]any{"data": results[1]})
	if err != nil {
		return nil, fmt.Errorf("subgraph failed: %w", err)
	}
	log.Printf("  Subgraph result: %v", subResult)

	return map[string]any{
		"extracted":  len(results),
		"transformed": transformResult,
		"sub":        subResult,
	}, nil
})

// ---------------------------------------------------------------------------
// 3. Subgraph — nested Graph inside Graph
// ---------------------------------------------------------------------------

var subPipeline = gi.Graph(gi.GraphOpts{
	Name:    "etl-sub-pipeline",
	Version: "1.0",
}, func(ctx context.Context) (any, error) {
	gCtx := gi.GraphRunContextFromOrPanic(ctx)
	log.Printf("  Subgraph: %s (run=%s, parent=%s)", gCtx.GraphName, gCtx.GraphRunID, gCtx.ParentGraphRunID)

	// Run transform → load sequentially
	data := gCtx.Parameters["data"].(map[string]any)
	cleaned, err := transform.Run(ctx, data)
	if err != nil {
		return nil, err
	}
	result, err := load.Run(ctx, cleaned)
	if err != nil {
		return nil, err
	}
	return result, nil
})

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.Println("=== GraphIngest Go SDK Demo ===")

	ctx := context.Background()
	result, err := pipeline.Run(ctx, nil)
	if err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}

	log.Printf("Final result: %v", result)
}
