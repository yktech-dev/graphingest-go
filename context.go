package graphingest

import (
	"context"
	"fmt"
)

// GraphRunContextData holds metadata for the current graph run.
type GraphRunContextData struct {
	GraphRunID       string
	GraphName        string
	GraphVersion     string
	Parameters       map[string]any
	Tags             []string
	ParentGraphRunID string
}

// NodeRunContextData holds metadata for the current node run.
type NodeRunContextData struct {
	NodeRunID  string
	NodeKey    string
	GraphRunID string
	MapIndex   *int
	RetryCount int
}

// context keys (unexported to prevent collisions)
type ctxKey int

const (
	graphRunCtxKey ctxKey = iota
	nodeRunCtxKey
)

// WithGraphRunContext returns a new context with the given GraphRunContextData.
func WithGraphRunContext(ctx context.Context, data *GraphRunContextData) context.Context {
	return context.WithValue(ctx, graphRunCtxKey, data)
}

// GraphRunContextFrom retrieves the GraphRunContextData from a context.
// Returns nil if not present.
func GraphRunContextFrom(ctx context.Context) *GraphRunContextData {
	v, _ := ctx.Value(graphRunCtxKey).(*GraphRunContextData)
	return v
}

// GraphRunContextFromOrPanic retrieves the GraphRunContextData or panics.
func GraphRunContextFromOrPanic(ctx context.Context) *GraphRunContextData {
	v := GraphRunContextFrom(ctx)
	if v == nil {
		panic("graphingest: not inside a Graph function — no GraphRunContext available")
	}
	return v
}

// WithNodeRunContext returns a new context with the given NodeRunContextData.
func WithNodeRunContext(ctx context.Context, data *NodeRunContextData) context.Context {
	return context.WithValue(ctx, nodeRunCtxKey, data)
}

// NodeRunContextFrom retrieves the NodeRunContextData from a context.
// Returns nil if not present.
func NodeRunContextFrom(ctx context.Context) *NodeRunContextData {
	v, _ := ctx.Value(nodeRunCtxKey).(*NodeRunContextData)
	return v
}

// NodeRunContextFromOrPanic retrieves the NodeRunContextData or panics.
func NodeRunContextFromOrPanic(ctx context.Context) *NodeRunContextData {
	v := NodeRunContextFrom(ctx)
	if v == nil {
		panic("graphingest: not inside a Node function — no NodeRunContext available")
	}
	return v
}

// String returns a human-readable representation of the graph run context.
func (g *GraphRunContextData) String() string {
	return fmt.Sprintf("GraphRun{id=%s, name=%s, version=%s}", g.GraphRunID, g.GraphName, g.GraphVersion)
}

// String returns a human-readable representation of the node run context.
func (n *NodeRunContextData) String() string {
	idx := "<nil>"
	if n.MapIndex != nil {
		idx = fmt.Sprintf("%d", *n.MapIndex)
	}
	return fmt.Sprintf("NodeRun{id=%s, key=%s, graphRun=%s, mapIndex=%s}", n.NodeRunID, n.NodeKey, n.GraphRunID, idx)
}
