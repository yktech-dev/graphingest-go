// Package graphingest provides a Go SDK for the GraphIngest Orchestrator.
//
// It communicates with the GraphIngest control plane (Next.js API routes)
// to report task state, upload logs, dispatch fan-out nodes, and store artifacts.
package graphingest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// GraphIngestClient communicates with the GraphIngest control plane.
type GraphIngestClient struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
}

// NewClient creates a new GraphIngestClient.
// If baseURL or apiKey are empty, they are read from GRAPHINGEST_API_URL / GRAPHINGEST_API_KEY
// environment variables (with INGEST_API_URL / INGEST_API_KEY as legacy fallbacks).
func NewClient(baseURL, apiKey string) *GraphIngestClient {
	if baseURL == "" {
		baseURL = os.Getenv("GRAPHINGEST_API_URL")
		if baseURL == "" {
			baseURL = os.Getenv("INGEST_API_URL")
		}
	}
	if apiKey == "" {
		apiKey = os.Getenv("GRAPHINGEST_API_KEY")
		if apiKey == "" {
			apiKey = os.Getenv("INGEST_API_KEY")
		}
	}
	return &GraphIngestClient{
		BaseURL: strings.TrimRight(baseURL, "/"),
		APIKey:  apiKey,
		HTTPClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// request performs an HTTP request and decodes the JSON response.
//
// The Authorization header depends on the path:
//   - /api/webhook/worker-callback uses WORKER_CALLBACK_SECRET
//     (a separate shared secret known only to in-cluster worker images).
//   - everything else uses the per-user API key.
func (c *GraphIngestClient) request(method, path string, body any, result any) error {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("graphingest: marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, c.BaseURL+path, bodyReader)
	if err != nil {
		return fmt.Errorf("graphingest: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	if strings.HasPrefix(path, "/api/webhook/") {
		secret := os.Getenv("WORKER_CALLBACK_SECRET")
		if secret == "" {
			return fmt.Errorf(
				"graphingest: %s requires WORKER_CALLBACK_SECRET in env", path,
			)
		}
		req.Header.Set("Authorization", "Bearer "+secret)
	} else {
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("graphingest: request %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("graphingest: API error %d: %s", resp.StatusCode, string(respBody))
	}

	if result != nil {
		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			return fmt.Errorf("graphingest: decode response: %w", err)
		}
	}
	return nil
}

// TaskCompletedPayload is the payload for reporting a completed task.
type TaskCompletedPayload struct {
	TaskRunID  string         `json:"task_run_id"`
	FlowRunID  string         `json:"flow_run_id"`
	Status     string         `json:"status"`
	ResultURL  string         `json:"result_url,omitempty"`
	ResultData map[string]any `json:"result_data,omitempty"`
	DurationMs int64          `json:"duration_ms,omitempty"`
	Logs       []LogEntry     `json:"logs,omitempty"`
	Artifacts  []ArtifactInput `json:"artifacts,omitempty"`
}

// TaskFailedPayload is the payload for reporting a failed task.
type TaskFailedPayload struct {
	TaskRunID      string     `json:"task_run_id"`
	FlowRunID      string     `json:"flow_run_id"`
	Status         string     `json:"status"`
	ErrorMessage   string     `json:"error_message"`
	ErrorTraceback string     `json:"error_traceback,omitempty"`
	Logs           []LogEntry `json:"logs,omitempty"`
}

// LogEntry represents a single log entry sent to the control plane.
type LogEntry struct {
	FlowRunID string         `json:"flow_run_id,omitempty"`
	TaskRunID string         `json:"task_run_id,omitempty"`
	Level     string         `json:"level"`
	Message   string         `json:"message"`
	WorkerID  string         `json:"worker_id,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// ArtifactInput represents an artifact to store.
type ArtifactInput struct {
	Key         string `json:"key"`
	Type        string `json:"type,omitempty"`
	Data        any    `json:"data,omitempty"`
	Description string `json:"description,omitempty"`
	StorageURL  string `json:"storage_url,omitempty"`
}

// ReportTaskCompleted reports a task as completed to the control plane.
func (c *GraphIngestClient) ReportTaskCompleted(p TaskCompletedPayload) error {
	p.Status = "COMPLETED"
	return c.request("POST", "/api/webhook/worker-callback", p, nil)
}

// ReportTaskFailed reports a task as failed to the control plane.
func (c *GraphIngestClient) ReportTaskFailed(p TaskFailedPayload) error {
	p.Status = "FAILED"
	return c.request("POST", "/api/webhook/worker-callback", p, nil)
}

// SendLogs sends log entries to the control plane for live streaming.
func (c *GraphIngestClient) SendLogs(flowRunID string, logs []LogEntry) error {
	return c.request("POST", fmt.Sprintf("/api/runs/%s/logs", flowRunID), logs, nil)
}

// CreateArtifact creates a rich artifact (markdown, table, plotly, etc).
func (c *GraphIngestClient) CreateArtifact(flowRunID string, artifact ArtifactInput) error {
	return c.request("POST", fmt.Sprintf("/api/runs/%s/artifacts", flowRunID), artifact, nil)
}

// TriggerFlowRun triggers a new flow run.
func (c *GraphIngestClient) TriggerFlowRun(flowID string, parameters map[string]any) (map[string]any, error) {
	var result map[string]any
	err := c.request("POST", fmt.Sprintf("/api/flows/%s/runs", flowID), map[string]any{
		"parameters": parameters,
	}, &result)
	return result, err
}

// DispatchNodesResult is the response from dispatching nodes.
type DispatchNodesResult struct {
	TaskRunIDs []string `json:"taskRunIds"`
	BarrierKey string   `json:"barrierKey,omitempty"`
	Count      int      `json:"count"`
}

// DispatchNodes dispatches one or more node executions within a graph run.
func (c *GraphIngestClient) DispatchNodes(graphRunID, nodeKey string, inputs []any) (*DispatchNodesResult, error) {
	var result DispatchNodesResult
	err := c.request("POST", "/api/nodes/dispatch", map[string]any{
		"graphRunId": graphRunID,
		"nodeKey":    nodeKey,
		"inputs":     inputs,
	}, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// TaskRunStatus represents the status of a single task run.
type TaskRunStatus struct {
	ID           string `json:"id"`
	State        string `json:"state"`
	ResultData   any    `json:"resultData"`
	ResultURL    string `json:"resultUrl"`
	ErrorMessage string `json:"errorMessage"`
	MapIndex     *int   `json:"mapIndex"`
}

// PollTaskRunsResult is the response from polling task run status.
type PollTaskRunsResult struct {
	Results      []TaskRunStatus `json:"results"`
	AllCompleted bool            `json:"allCompleted"`
}

// PollTaskRuns checks the status of task runs.
func (c *GraphIngestClient) PollTaskRuns(taskRunIDs []string) (*PollTaskRunsResult, error) {
	var result PollTaskRunsResult
	err := c.request("POST", "/api/nodes/status", map[string]any{
		"taskRunIds": taskRunIDs,
	}, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// defaultClient is a lazily-initialized singleton.
var defaultClient *GraphIngestClient

// GetClient returns the default singleton client.
func GetClient() *GraphIngestClient {
	if defaultClient == nil {
		defaultClient = NewClient("", "")
	}
	return defaultClient
}
