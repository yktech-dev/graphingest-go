# GraphIngest Go SDK

Go SDK for the [GraphIngest Orchestrator](../../README.md) — define pipeline nodes and graphs with typed Go functions.

## Installation

```bash
go get github.com/yktech-dev/graphingest-go
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    gi "github.com/yktech-dev/graphingest-go"
)

// Define nodes with typed input/output
var extract = gi.Node(gi.NodeOpts{Name: "extract"}, func(ctx context.Context, url string) (map[string]any, error) {
    return map[string]any{"url": url, "rows": 100}, nil
})

var transform = gi.Node(gi.NodeOpts{Name: "transform"}, func(ctx context.Context, data map[string]any) (map[string]any, error) {
    data["cleaned"] = true
    return data, nil
})

// Define a graph with retry policy
var pipeline = gi.Graph(gi.GraphOpts{
    Name: "etl-pipeline",
    RetryPolicy: gi.RetryPolicy{
        MaxRetries:    3,
        DelaySeconds:  1.0,
        BackoffFactor: 2.0,
        Jitter:        true,
    },
    TimeoutSeconds: 300,
}, func(ctx context.Context) (any, error) {
    data, err := extract.Run(ctx, "https://api.example.com/data")
    if err != nil {
        return nil, err
    }
    return transform.Run(ctx, data)
})

func main() {
    ctx := context.Background()
    result, err := pipeline.Run(ctx, nil)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(result)
}
```

## Deploy

Push your code to the platform:

```go
import gi "github.com/yktech-dev/graphingest-go"

// Dashboard-only (no local env file):
result, err := gi.Deploy(gi.DeployOpts{})

// With a local .env file:
result, err := gi.Deploy(gi.DeployOpts{EnvPath: ".env"})

// .env.local or absolute path:
result, err := gi.Deploy(gi.DeployOpts{EnvPath: ".env.local"})
result, err := gi.Deploy(gi.DeployOpts{EnvPath: "/home/me/prod.env"})
```

Dashboard variables always take precedence over file variables at runtime.

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `GRAPHINGEST_API_URL` | Yes | Control plane URL |
| `GRAPHINGEST_API_KEY` | Yes | API key |
| `GRAPH_RUN_ID` | Auto | Set by worker |
| `NODE_RUN_ID` | Auto | Set by worker |

Legacy env vars (`INGEST_API_URL`, `INGEST_API_KEY`, `FLOW_RUN_ID`, `TASK_RUN_ID`) are supported as fallbacks.

You can also manage env vars in the dashboard — upload `.env` files, paste multiple vars, or add them one by one.

## API Reference

### Node

```go
// Create a typed node
var myNode = gi.Node(gi.NodeOpts{
    Name:     "my-node",
    CacheTTL: 3600,        // optional: cache results for 1 hour
}, func(ctx context.Context, input InputType) (OutputType, error) {
    return result, nil
})

// Run directly
result, err := myNode.Run(ctx, input)

// Fan-out: parallel dispatch
results, err := myNode.Map(ctx, []InputType{a, b, c}, &gi.MapOpts{
    PollInterval: 500 * time.Millisecond,
    Timeout:      60 * time.Second,
})

// Async dispatch: fire-and-forget with future
future, err := myNode.ARun(ctx, input)
result, err := future.Result(&gi.ResultOpts{Timeout: 30 * time.Second})
```

### Graph

```go
var myGraph = gi.Graph(gi.GraphOpts{
    Name:           "my-graph",
    Version:        "1.0",
    Tags:           []string{"prod"},
    TimeoutSeconds: 600,
    RetryPolicy: gi.RetryPolicy{
        MaxRetries:      3,
        DelaySeconds:    1.0,
        BackoffFactor:   2.0,
        MaxDelaySeconds: 120.0,
        Jitter:          true,
    },
    OnCompletion: func(ctx *gi.GraphRunContextData, result any) { ... },
    OnFailure:    func(ctx *gi.GraphRunContextData, err error) { ... },
}, func(ctx context.Context) (any, error) {
    // Access context
    gCtx := gi.GraphRunContextFromOrPanic(ctx)
    fmt.Println(gCtx.GraphRunID, gCtx.GraphName)

    // Orchestrate nodes
    data, _ := extract.Run(ctx, "url")
    return transform.Run(ctx, data)
})

// Run the graph
result, err := myGraph.Run(context.Background(), map[string]any{"key": "value"})
```

### Subgraphs

Call a graph from inside another graph — it automatically detects the parent and links run IDs:

```go
var parentGraph = gi.Graph(gi.GraphOpts{Name: "parent"}, func(ctx context.Context) (any, error) {
    // This creates a subgraph with its own run ID, linked to parent
    return childGraph.Run(ctx, nil)
})

var childGraph = gi.Graph(gi.GraphOpts{Name: "child"}, func(ctx context.Context) (any, error) {
    gCtx := gi.GraphRunContextFromOrPanic(ctx)
    fmt.Println(gCtx.ParentGraphRunID) // parent's run ID
    return "done", nil
})
```

### Context

```go
// Inside a graph function:
gCtx := gi.GraphRunContextFrom(ctx)      // nil if not in graph
gCtx := gi.GraphRunContextFromOrPanic(ctx) // panics if not in graph

gCtx.GraphRunID       // "uuid-..."
gCtx.GraphName        // "my-graph"
gCtx.GraphVersion     // "1.0"
gCtx.Parameters       // map[string]any
gCtx.Tags             // []string
gCtx.ParentGraphRunID // "" or parent's run ID

// Inside a node function:
nCtx := gi.NodeRunContextFrom(ctx)
nCtx.NodeRunID   // "uuid-..."
nCtx.NodeKey     // "extract"
nCtx.GraphRunID  // parent graph's run ID
nCtx.MapIndex    // *int (nil or index in .Map())
```

### AI Agent (ReAct)

```go
// Define tools from your node functions
searchTool := gi.ToolDef{
    Name:        "search",
    Description: "Search the web for information.",
    Parameters:  map[string]map[string]any{"query": {"type": "string"}},
    Fn: func(args map[string]any) (string, error) {
        query, _ := args["query"].(string)
        result, err := search.Run(ctx, query)
        return fmt.Sprintf("%v", result), err
    },
}

// One-liner agent: graph + ReAct loop
researcher := gi.Agent(gi.AgentOpts{
    Name:         "researcher",
    Tools:        []gi.ToolDef{searchTool},
    Model:        "standard",  // or "high", "gpt-4o", "claude-3.5-sonnet", etc.
    SystemPrompt: "You are a research assistant.",
})
answer, err := researcher.Run(ctx, "What is quantum computing?")

// Or use React() directly for more control
result, err := gi.React(gi.ReactOpts{
    Query:         "Research fusion energy",
    Tools:         []gi.ToolDef{searchTool, scrapeTool},
    Model:         "standard",
    MaxIterations: 10,
})
fmt.Println(result.Answer, result.Steps, result.ElapsedSeconds)
```

**Model tiers:**

| Model | Description |
|-------|-------------|
| `"standard"` | Fast and cost-effective (default, no API key needed) |
| `"high"` | Premium quality for complex reasoning (no API key needed) |
| `"gpt-4o"` | BYOK: OpenAI (set `OPENAI_API_KEY`) |
| `"claude-3.5-sonnet"` | BYOK: Anthropic (set `ANTHROPIC_API_KEY`) |
| `"gemini-2.5-flash"` | BYOK: Google (set `GOOGLE_API_KEY`) |

### Streaming Logger

```go
logger := gi.NewStreamingLogger(gi.StreamingLoggerOpts{
    FlowRunID:  graphRunID,
    BufferSize: 20,
    FlushEvery: 2 * time.Second,
})
defer logger.Close()

logger.Info("Starting extraction")
logger.Infof("Processing %d items", count)
logger.Warn("Rate limit approaching")
logger.Error("Connection failed")
```

### RetryPolicy

```go
gi.RetryPolicy{
    MaxRetries:      3,     // Total retry attempts
    DelaySeconds:    1.0,   // Initial delay
    BackoffFactor:   2.0,   // Multiplier per attempt
    MaxDelaySeconds: 120.0, // Upper bound
    Jitter:          true,  // ±50% randomization
}

// Delay formula: min(DelaySeconds * BackoffFactor^attempt, MaxDelaySeconds) × jitter
// Example with above config:
//   Attempt 0: ~1s (0.5-1.5s with jitter)
//   Attempt 1: ~2s (1.0-3.0s with jitter)
//   Attempt 2: ~4s (2.0-6.0s with jitter)
```

## Exports

| Export | Type | Description |
|--------|------|-------------|
| `Node` | function | Create a typed node wrapper |
| `Graph` | function | Create a graph wrapper |
| `Deploy` | function | Push code to platform (supports `EnvPath`) |
| `NewClient` | function | Create an API client |
| `GetClient` | function | Get singleton client |
| `NewNodeFuture` | function | Create async dispatch handle |
| `NewStreamingLogger` | function | Create streaming logger |
| `ComputeInputHash` | function | SHA-256 cache key |
| `GraphRunContextFrom` | function | Get graph context from ctx |
| `NodeRunContextFrom` | function | Get node context from ctx |
| `WithGraphRunContext` | function | Set graph context on ctx |
| `WithNodeRunContext` | function | Set node context on ctx |
| `DeployOpts` | struct | Deploy configuration |
| `DeployResult` | struct | Deploy response |
| `React` | function | ReAct loop: LLM + tool routing |
| `Agent` | function | One-liner agent combining Graph + React |
| `ToolsFromDefs` | function | Convert ToolDefs to LLM schemas |
| `RetryPolicy` | struct | Retry configuration |
| `GraphOpts` | struct | Graph configuration |
| `NodeOpts` | struct | Node configuration |
| `MapOpts` | struct | Fan-out options |
| `ResultOpts` | struct | Future poll options |
| `GraphTimeoutError` | struct | Timeout error type |
| `ReactOpts` | struct | React loop configuration |
| `ReactResult` | struct | React loop result |
| `AgentOpts` | struct | Agent configuration |
| `ToolDef` | struct | Tool definition for LLM |
| `ToolSchema` | struct | LLM-ready tool schema |
