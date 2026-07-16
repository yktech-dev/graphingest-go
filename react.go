package graphingest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

// ToolCall represents a normalized tool call from any LLM provider.
type ToolCall struct {
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

// LLMResponse is a normalized response from any LLM provider.
type LLMResponse struct {
	Content   string     `json:"content"`
	ToolCalls []ToolCall `json:"tool_calls"`
}

// ReactResult is the result of a React() loop execution.
type ReactResult struct {
	Answer         string             `json:"answer"`
	ToolCallLog    []ToolCallLogEntry `json:"tool_calls"`
	Steps          int                `json:"steps"`
	ElapsedSeconds float64            `json:"elapsed_seconds"`
	Model          string             `json:"model"`
}

// ToolCallLogEntry records a single tool execution.
type ToolCallLogEntry struct {
	Tool   string `json:"tool"`
	Args   any    `json:"args"`
	Result string `json:"result"`
}

// ToolSchema describes a tool in OpenAI function-calling format.
type ToolSchema struct {
	Type     string             `json:"type"`
	Function ToolSchemaFunction `json:"function"`
}

// ToolSchemaFunction is the function definition inside a ToolSchema.
type ToolSchemaFunction struct {
	Name        string                `json:"name"`
	Description string                `json:"description"`
	Parameters  ToolSchemaParameters  `json:"parameters"`
}

// ToolSchemaParameters describes the JSON Schema for function parameters.
type ToolSchemaParameters struct {
	Type       string                    `json:"type"`
	Properties map[string]map[string]any `json:"properties"`
	Required   []string                  `json:"required"`
}

// ToolDef registers a node function as an LLM tool.
type ToolDef struct {
	// Name must match the node's Name.
	Name string
	// Description shown to the LLM.
	Description string
	// Parameters describes each parameter (name → JSON Schema property).
	Parameters map[string]map[string]any
	// Required parameter names.
	Required []string
	// Fn executes the tool and returns a string result.
	Fn func(args map[string]any) (string, error)
}

// ---------------------------------------------------------------------------
// ToolsFromDefs: convert ToolDef slice to ToolSchema slice
// ---------------------------------------------------------------------------

// ToolsFromDefs converts tool definitions into LLM-ready tool schemas.
func ToolsFromDefs(defs []ToolDef) []ToolSchema {
	schemas := make([]ToolSchema, len(defs))
	for i, d := range defs {
		props := d.Parameters
		if props == nil {
			props = map[string]map[string]any{"input": {"type": "string"}}
		}
		req := d.Required
		if req == nil {
			req = make([]string, 0, len(props))
			for k := range props {
				req = append(req, k)
			}
		}
		schemas[i] = ToolSchema{
			Type: "function",
			Function: ToolSchemaFunction{
				Name:        d.Name,
				Description: d.Description,
				Parameters: ToolSchemaParameters{
					Type:       "object",
					Properties: props,
					Required:   req,
				},
			},
		}
	}
	return schemas
}

// ---------------------------------------------------------------------------
// LLM Provider abstraction
// ---------------------------------------------------------------------------

type llmProvider interface {
	chat(messages []map[string]any, tools []ToolSchema, temperature float64, model string) (*LLMResponse, error)
	appendAssistant(messages *[]map[string]any, response *LLMResponse)
	appendToolResult(messages *[]map[string]any, tc ToolCall, result string)
}

// ---------------------------------------------------------------------------
// OpenAI provider (also used for platform proxy)
// ---------------------------------------------------------------------------

type openaiProvider struct {
	baseURL string
	apiKey  string
}

func (p *openaiProvider) chat(messages []map[string]any, tools []ToolSchema, temperature float64, model string) (*LLMResponse, error) {
	url := p.baseURL + "/chat/completions"
	key := p.apiKey
	if key == "" {
		key = os.Getenv("OPENAI_API_KEY")
	}

	body := map[string]any{
		"model":       model,
		"messages":    messages,
		"temperature": temperature,
	}
	if len(tools) > 0 {
		body["tools"] = tools
	}

	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("graphingest/react: marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("graphingest/react: LLM request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("graphingest/react: LLM error %d: %s", resp.StatusCode, string(b))
	}

	var result struct {
		Choices []struct {
			Message struct {
				Content   *string `json:"content"`
				ToolCalls []struct {
					ID       string `json:"id"`
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("graphingest/react: decode LLM response: %w", err)
	}

	llmResp := &LLMResponse{}
	if len(result.Choices) > 0 {
		msg := result.Choices[0].Message
		if msg.Content != nil {
			llmResp.Content = *msg.Content
		}
		for _, tc := range msg.ToolCalls {
			var args map[string]any
			_ = json.Unmarshal([]byte(tc.Function.Arguments), &args)
			llmResp.ToolCalls = append(llmResp.ToolCalls, ToolCall{
				ID:        tc.ID,
				Name:      tc.Function.Name,
				Arguments: args,
			})
		}
	}
	return llmResp, nil
}

func (p *openaiProvider) appendAssistant(messages *[]map[string]any, response *LLMResponse) {
	msg := map[string]any{"role": "assistant"}
	if response.Content != "" {
		msg["content"] = response.Content
	}
	if len(response.ToolCalls) > 0 {
		tcs := make([]map[string]any, len(response.ToolCalls))
		for i, tc := range response.ToolCalls {
			argsJSON, _ := json.Marshal(tc.Arguments)
			tcs[i] = map[string]any{
				"id":   tc.ID,
				"type": "function",
				"function": map[string]any{
					"name":      tc.Name,
					"arguments": string(argsJSON),
				},
			}
		}
		msg["tool_calls"] = tcs
	}
	*messages = append(*messages, msg)
}

func (p *openaiProvider) appendToolResult(messages *[]map[string]any, tc ToolCall, result string) {
	*messages = append(*messages, map[string]any{
		"role":         "tool",
		"tool_call_id": tc.ID,
		"content":      result,
	})
}

// ---------------------------------------------------------------------------
// Anthropic provider
// ---------------------------------------------------------------------------

type anthropicProvider struct{}

func (p *anthropicProvider) chat(messages []map[string]any, tools []ToolSchema, temperature float64, model string) (*LLMResponse, error) {
	key := os.Getenv("ANTHROPIC_API_KEY")

	anthropicTools := make([]map[string]any, len(tools))
	for i, t := range tools {
		anthropicTools[i] = map[string]any{
			"name":         t.Function.Name,
			"description":  t.Function.Description,
			"input_schema": t.Function.Parameters,
		}
	}

	system := ""
	filtered := make([]map[string]any, 0, len(messages))
	for _, m := range messages {
		if m["role"] == "system" {
			system, _ = m["content"].(string)
		} else {
			filtered = append(filtered, m)
		}
	}

	body := map[string]any{
		"model":       model,
		"messages":    filtered,
		"max_tokens":  4096,
		"temperature": temperature,
	}
	if len(anthropicTools) > 0 {
		body["tools"] = anthropicTools
	}
	if system != "" {
		body["system"] = system
	}

	data, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "https://api.anthropic.com/v1/messages", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", key)
	req.Header.Set("anthropic-version", "2023-06-01")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("graphingest/react: Anthropic request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("graphingest/react: Anthropic error %d: %s", resp.StatusCode, string(b))
	}

	var result struct {
		Content []struct {
			Type  string         `json:"type"`
			Text  string         `json:"text,omitempty"`
			ID    string         `json:"id,omitempty"`
			Name  string         `json:"name,omitempty"`
			Input map[string]any `json:"input,omitempty"`
		} `json:"content"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	llmResp := &LLMResponse{}
	for _, block := range result.Content {
		switch block.Type {
		case "text":
			llmResp.Content += block.Text
		case "tool_use":
			llmResp.ToolCalls = append(llmResp.ToolCalls, ToolCall{
				ID:        block.ID,
				Name:      block.Name,
				Arguments: block.Input,
			})
		}
	}
	return llmResp, nil
}

func (p *anthropicProvider) appendAssistant(messages *[]map[string]any, response *LLMResponse) {
	blocks := []map[string]any{}
	if response.Content != "" {
		blocks = append(blocks, map[string]any{"type": "text", "text": response.Content})
	}
	for _, tc := range response.ToolCalls {
		blocks = append(blocks, map[string]any{
			"type":  "tool_use",
			"id":    tc.ID,
			"name":  tc.Name,
			"input": tc.Arguments,
		})
	}
	*messages = append(*messages, map[string]any{"role": "assistant", "content": blocks})
}

func (p *anthropicProvider) appendToolResult(messages *[]map[string]any, tc ToolCall, result string) {
	*messages = append(*messages, map[string]any{
		"role": "user",
		"content": []map[string]any{
			{"type": "tool_result", "tool_use_id": tc.ID, "content": result},
		},
	})
}

// ---------------------------------------------------------------------------
// Gemini provider
// ---------------------------------------------------------------------------

type geminiProvider struct{}

func (p *geminiProvider) chat(messages []map[string]any, tools []ToolSchema, temperature float64, model string) (*LLMResponse, error) {
	key := os.Getenv("GOOGLE_API_KEY")

	decls := make([]map[string]any, len(tools))
	for i, t := range tools {
		decls[i] = map[string]any{
			"name":        t.Function.Name,
			"description": t.Function.Description,
			"parameters":  t.Function.Parameters,
		}
	}

	var systemInstruction *map[string]any
	contents := []map[string]any{}
	for _, m := range messages {
		role, _ := m["role"].(string)
		switch role {
		case "system":
			si := map[string]any{"parts": []map[string]any{{"text": m["content"]}}}
			systemInstruction = &si
		case "user":
			contents = append(contents, map[string]any{
				"role":  "user",
				"parts": []map[string]any{{"text": m["content"]}},
			})
		case "assistant":
			parts := []map[string]any{}
			if c, _ := m["content"].(string); c != "" {
				parts = append(parts, map[string]any{"text": c})
			}
			if tcs, ok := m["_toolCalls"].([]map[string]any); ok {
				for _, tc := range tcs {
					parts = append(parts, map[string]any{
						"functionCall": map[string]any{"name": tc["name"], "args": tc["arguments"]},
					})
				}
			}
			if len(parts) > 0 {
				contents = append(contents, map[string]any{"role": "model", "parts": parts})
			}
		case "tool_result":
			contents = append(contents, map[string]any{
				"role": "user",
				"parts": []map[string]any{{
					"functionResponse": map[string]any{
						"name":     m["name"],
						"response": map[string]any{"result": m["content"]},
					},
				}},
			})
		}
	}

	body := map[string]any{
		"contents":         contents,
		"generationConfig": map[string]any{"temperature": temperature},
	}
	if len(decls) > 0 {
		body["tools"] = []map[string]any{{"functionDeclarations": decls}}
	}
	if systemInstruction != nil {
		body["systemInstruction"] = *systemInstruction
	}

	url := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent?key=%s", model, key)
	data, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", url, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("graphingest/react: Gemini request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("graphingest/react: Gemini error %d: %s", resp.StatusCode, string(b))
	}

	var result struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text         string          `json:"text,omitempty"`
					FunctionCall *struct {
						Name string         `json:"name"`
						Args map[string]any `json:"args"`
					} `json:"functionCall,omitempty"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	llmResp := &LLMResponse{}
	if len(result.Candidates) > 0 {
		for _, part := range result.Candidates[0].Content.Parts {
			if part.Text != "" {
				llmResp.Content += part.Text
			}
			if part.FunctionCall != nil {
				llmResp.ToolCalls = append(llmResp.ToolCalls, ToolCall{
					ID:        fmt.Sprintf("call_%08x", rand.Int31()),
					Name:      part.FunctionCall.Name,
					Arguments: part.FunctionCall.Args,
				})
			}
		}
	}
	return llmResp, nil
}

func (p *geminiProvider) appendAssistant(messages *[]map[string]any, response *LLMResponse) {
	tcs := make([]map[string]any, len(response.ToolCalls))
	for i, tc := range response.ToolCalls {
		tcs[i] = map[string]any{"name": tc.Name, "arguments": tc.Arguments}
	}
	*messages = append(*messages, map[string]any{
		"role":       "assistant",
		"content":    response.Content,
		"_toolCalls": tcs,
	})
}

func (p *geminiProvider) appendToolResult(messages *[]map[string]any, tc ToolCall, result string) {
	*messages = append(*messages, map[string]any{
		"role":    "tool_result",
		"name":    tc.Name,
		"content": result,
	})
}

// ---------------------------------------------------------------------------
// Provider auto-detection
// ---------------------------------------------------------------------------

var platformTiers = map[string]string{
	"standard": "gemini-2.5-flash",
	"high":     "gemini-2.5-pro",
}

func getProvider(model string) (llmProvider, string) {
	if resolved, ok := platformTiers[model]; ok {
		platformURL := os.Getenv("GRAPHINGEST_API_URL")
		apiKey := os.Getenv("GRAPHINGEST_API_KEY")
		if platformURL == "" {
			panic(fmt.Sprintf("graphingest/react: model=%q requires GRAPHINGEST_API_URL", model))
		}
		return &openaiProvider{
			baseURL: platformURL + "/llm/v1",
			apiKey:  apiKey,
		}, resolved
	}

	lower := strings.ToLower(model)
	if strings.HasPrefix(lower, "gpt-") || strings.HasPrefix(lower, "o1") ||
		strings.HasPrefix(lower, "o3") || strings.HasPrefix(lower, "o4") {
		return &openaiProvider{baseURL: "https://api.openai.com/v1"}, model
	}
	if strings.HasPrefix(lower, "claude") {
		return &anthropicProvider{}, model
	}
	if strings.HasPrefix(lower, "gemini") {
		return &geminiProvider{}, model
	}

	// Default: OpenAI-compatible (Together, Groq, etc.)
	return &openaiProvider{baseURL: "https://api.openai.com/v1"}, model
}

// ---------------------------------------------------------------------------
// ReactOpts configures a React() call.
// ---------------------------------------------------------------------------

// ReactOpts configures the React loop.
type ReactOpts struct {
	// Query is the user's question or task.
	Query string
	// Tools are the tool definitions available to the LLM.
	Tools []ToolDef
	// Model: "standard", "high", or a BYOK model name (default: "standard").
	Model string
	// SystemPrompt is prepended to the conversation.
	SystemPrompt string
	// MaxIterations limits reasoning loops (default: 10).
	MaxIterations int
	// Temperature for the LLM (default: 0).
	Temperature float64
}

// React runs a ReAct loop: LLM reasons → picks tools → tools execute → repeat.
//
// The LLM automatically decides which tools to call based on the query.
// Each tool call is routed to the registered Fn in the ToolDef.
//
// Supported models:
//
//	Platform-managed (no API key needed):
//	  "standard" — fast and cost-effective (default)
//	  "high"     — premium quality
//
//	BYOK (bring your own API key):
//	  "gpt-4o", "claude-3.5-sonnet", "gemini-2.5-flash", etc.
//
// Example:
//
//	result, err := gi.React(gi.ReactOpts{
//	    Query: "Research fusion energy",
//	    Tools: []gi.ToolDef{searchTool},
//	    Model: "standard",
//	})
func React(opts ReactOpts) (*ReactResult, error) {
	model := opts.Model
	if model == "" {
		model = "standard"
	}
	maxIter := opts.MaxIterations
	if maxIter <= 0 {
		maxIter = 10
	}

	provider, resolvedModel := getProvider(model)
	schemas := ToolsFromDefs(opts.Tools)

	// Build tool lookup
	toolMap := map[string]ToolDef{}
	for _, t := range opts.Tools {
		toolMap[t.Name] = t
	}

	// Initial messages
	messages := []map[string]any{}
	if opts.SystemPrompt != "" {
		messages = append(messages, map[string]any{"role": "system", "content": opts.SystemPrompt})
	}
	messages = append(messages, map[string]any{"role": "user", "content": opts.Query})

	var allToolCalls []ToolCallLogEntry
	start := time.Now()

	for step := 0; step < maxIter; step++ {
		response, err := provider.chat(messages, schemas, opts.Temperature, resolvedModel)
		if err != nil {
			return nil, fmt.Errorf("graphingest/react: LLM call failed at step %d: %w", step, err)
		}

		provider.appendAssistant(&messages, response)

		// No tool calls → done
		if len(response.ToolCalls) == 0 {
			return &ReactResult{
				Answer:         response.Content,
				ToolCallLog:    allToolCalls,
				Steps:          step + 1,
				ElapsedSeconds: time.Since(start).Seconds(),
				Model:          model,
			}, nil
		}

		// Execute tool calls
		for _, tc := range response.ToolCalls {
			tool, ok := toolMap[tc.Name]
			var toolResult string
			if !ok {
				toolResult = fmt.Sprintf("Unknown tool: %s", tc.Name)
			} else {
				res, err := tool.Fn(tc.Arguments)
				if err != nil {
					toolResult = fmt.Sprintf("Error: %v", err)
				} else {
					toolResult = res
				}
			}

			allToolCalls = append(allToolCalls, ToolCallLogEntry{
				Tool:   tc.Name,
				Args:   tc.Arguments,
				Result: toolResult,
			})
			provider.appendToolResult(&messages, tc, toolResult)
		}
	}

	return &ReactResult{
		Answer:         fmt.Sprintf("Max iterations (%d) reached without a final answer.", maxIter),
		ToolCallLog:    allToolCalls,
		Steps:          maxIter,
		ElapsedSeconds: time.Since(start).Seconds(),
		Model:          model,
	}, nil
}

// ---------------------------------------------------------------------------
// Agent: combines Graph + React in one call
// ---------------------------------------------------------------------------

// AgentOpts configures an Agent.
type AgentOpts struct {
	// Name shown in dashboard.
	Name string
	// Tools available to the LLM.
	Tools []ToolDef
	// Model: "standard", "high", or BYOK model name (default: "standard").
	Model string
	// SystemPrompt for the LLM.
	SystemPrompt string
	// MaxIterations for the ReAct loop (default: 10).
	MaxIterations int
	// Temperature for the LLM (default: 0).
	Temperature float64
	// TimeoutSeconds for the graph wrapper (default: 600).
	TimeoutSeconds int
	// RetryPolicy for the graph wrapper.
	RetryPolicy RetryPolicy
	// Tags for dashboard metadata.
	Tags []string
}

// AgentFunc is a convenience wrapper that runs React inside a Graph.
// Call Agent.Run(ctx, query) to execute.
type AgentFunc struct {
	opts AgentOpts
	gw   *GraphWrapper
}

// Agent creates an AI agent backed by tool definitions.
//
// Combines Graph() with a built-in ReAct loop. The LLM automatically
// decides which tools to call.
//
// Example:
//
//	researcher := gi.Agent(gi.AgentOpts{
//	    Name:         "researcher",
//	    Tools:        []gi.ToolDef{searchTool, scrapeTool},
//	    Model:        "standard",
//	    SystemPrompt: "You are a research assistant.",
//	})
//	answer, err := researcher.Run(ctx, "What is quantum computing?")
func Agent(opts AgentOpts) *AgentFunc {
	if opts.Name == "" {
		panic("graphingest: AgentOpts.Name is required")
	}
	if opts.TimeoutSeconds <= 0 {
		opts.TimeoutSeconds = 600
	}
	maxIter := opts.MaxIterations
	if maxIter <= 0 {
		maxIter = 10
	}

	gw := Graph(GraphOpts{
		Name:           opts.Name,
		Tags:           opts.Tags,
		TimeoutSeconds: opts.TimeoutSeconds,
		RetryPolicy:    opts.RetryPolicy,
	}, func(ctx context.Context) (any, error) {
		// query is injected via parameters at runtime
		return nil, nil // placeholder; actual logic in AgentFunc.Run
	})

	return &AgentFunc{opts: opts, gw: gw}
}

// Run executes the agent with the given query.
func (a *AgentFunc) Run(ctx context.Context, query string) (string, error) {
	model := a.opts.Model
	if model == "" {
		model = "standard"
	}
	maxIter := a.opts.MaxIterations
	if maxIter <= 0 {
		maxIter = 10
	}

	result, err := React(ReactOpts{
		Query:         query,
		Tools:         a.opts.Tools,
		Model:         model,
		SystemPrompt:  a.opts.SystemPrompt,
		MaxIterations: maxIter,
		Temperature:   a.opts.Temperature,
	})
	if err != nil {
		return "", err
	}

	log.Printf("[agent:%s] Completed in %.2fs (%d steps, %d tool calls)",
		a.opts.Name, result.ElapsedSeconds, result.Steps, len(result.ToolCallLog))

	return result.Answer, nil
}
