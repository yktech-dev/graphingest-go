package graphingest

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// DeployOpts configures the deploy() call.
type DeployOpts struct {
	// EnvPath is an optional path to a .env file (relative or absolute).
	// If empty, env vars are read from the dashboard only.
	EnvPath string

	// ProjectDir is the project root directory. Defaults to current working directory.
	ProjectDir string
}

// DeployResult is the response from the platform after a successful deploy.
type DeployResult struct {
	Functions        []string `json:"functions"`
	DashboardEnvVars []string `json:"dashboard_env_vars,omitempty"`
}

var decoratorRE = regexp.MustCompile(`(?:Node|Graph)\s*\(`)

var skipDirs = map[string]bool{
	"vendor":        true,
	"node_modules":  true,
	".git":          true,
	".venv":         true,
	"__pycache__":   true,
	"testdata":      true,
}

// Deploy pushes your code to the GraphIngest platform.
//
// Scans for .go files containing Node()/Graph() calls, optionally reads
// a .env file, and uploads everything. The platform builds an execution
// environment and makes your functions available for .Run(), .Map(), .Submit().
//
// Environment variables:
//   - If EnvPath is provided, reads that file and uploads those variables.
//     Dashboard variables with the same key take precedence at runtime.
//   - If EnvPath is empty, all env vars come from the dashboard.
//
// Examples:
//
//	// Dashboard-only:
//	result, err := graphingest.Deploy(graphingest.DeployOpts{})
//
//	// With a local .env file:
//	result, err := graphingest.Deploy(graphingest.DeployOpts{EnvPath: ".env"})
//
//	// Absolute path:
//	result, err := graphingest.Deploy(graphingest.DeployOpts{EnvPath: "/home/me/prod.env"})
func Deploy(opts DeployOpts) (*DeployResult, error) {
	projectDir := opts.ProjectDir
	if projectDir == "" {
		var err error
		projectDir, err = os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("graphingest: get working directory: %w", err)
		}
	}

	// 1. Find source files with Node()/Graph()
	log.Println("Scanning for Node()/Graph() functions...")
	sourceFiles, err := findGoSourceFiles(projectDir)
	if err != nil {
		return nil, err
	}
	if len(sourceFiles) == 0 {
		return nil, fmt.Errorf("graphingest: no Go files with Node() or Graph() found in %s", projectDir)
	}
	log.Printf("  Found %d file(s) with Node()/Graph() calls", len(sourceFiles))

	// 2. Read env file (only if EnvPath provided)
	envVars := map[string]string{}
	if opts.EnvPath != "" {
		resolved := opts.EnvPath
		if !filepath.IsAbs(resolved) {
			resolved = filepath.Join(projectDir, resolved)
		}
		envVars, err = parseEnvFileGo(resolved)
		if err != nil {
			log.Printf("  Warning: could not read %s: %v", opts.EnvPath, err)
		}
		if len(envVars) > 0 {
			log.Printf("Environment variables (from %s):", opts.EnvPath)
			keys := sortedKeys(envVars)
			for _, key := range keys {
				log.Printf("  ✓ %s", key)
			}
		} else {
			log.Printf("  Warning: %s not found or empty — using dashboard variables only", opts.EnvPath)
		}
	} else {
		log.Println("  No EnvPath provided — using dashboard variables only")
	}

	// 3. Read go.mod for module info
	goModPath := filepath.Join(projectDir, "go.mod")
	var goMod string
	if data, err := os.ReadFile(goModPath); err == nil {
		goMod = string(data)
		log.Println("  Found go.mod")
	} else {
		log.Println("  No go.mod found")
	}

	// 4. Prepare payload
	files := map[string]string{}
	for _, fp := range sourceFiles {
		rel, _ := filepath.Rel(projectDir, fp)
		data, err := os.ReadFile(fp)
		if err != nil {
			continue
		}
		files[rel] = string(data)
	}

	payload := map[string]any{
		"files":    files,
		"go_mod":   goMod,
		"env_vars": envVars,
		"language": "go",
	}

	// 5. Upload to platform
	log.Println("Uploading to GraphIngest platform...")
	client := GetClient()

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("graphingest: marshal deploy payload: %w", err)
	}

	req, err := http.NewRequest("POST", client.BaseURL+"/api/deploy", strings.NewReader(string(payloadBytes)))
	if err != nil {
		return nil, fmt.Errorf("graphingest: create deploy request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+client.APIKey)

	httpClient := &http.Client{Timeout: 300 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("graphingest: deploy request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body := make([]byte, 4096)
		n, _ := resp.Body.Read(body)
		return nil, fmt.Errorf("graphingest: deploy failed (%d): %s", resp.StatusCode, string(body[:n]))
	}

	var result DeployResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("graphingest: decode deploy response: %w", err)
	}

	// 6. Show dashboard env var summary
	if len(result.DashboardEnvVars) > 0 {
		dashboardOnly := []string{}
		overrides := []string{}
		for _, k := range result.DashboardEnvVars {
			if _, exists := envVars[k]; exists {
				overrides = append(overrides, k)
			} else {
				dashboardOnly = append(dashboardOnly, k)
			}
		}
		if len(dashboardOnly) > 0 {
			log.Println("Dashboard variables:")
			sort.Strings(dashboardOnly)
			for _, key := range dashboardOnly {
				log.Printf("  ✓ %s", key)
			}
		}
		if len(overrides) > 0 {
			log.Println("Dashboard overrides (take precedence over env file):")
			sort.Strings(overrides)
			for _, key := range overrides {
				log.Printf("  ⚠ %s", key)
			}
		}
	}

	// 7. Report success
	log.Printf("Deployed. %d function(s) registered:", len(result.Functions))
	for _, fn := range result.Functions {
		log.Printf("  • %s", fn)
	}

	return &result, nil
}

// findGoSourceFiles walks the project and returns .go files containing Node() or Graph().
func findGoSourceFiles(root string) ([]string, error) {
	var matches []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // skip unreadable
		}
		name := d.Name()
		if d.IsDir() {
			if strings.HasPrefix(name, ".") || skipDirs[name] {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		if decoratorRE.Match(data) {
			matches = append(matches, path)
		}
		return nil
	})
	return matches, err
}

// parseEnvFileGo reads a .env file into key-value pairs.
func parseEnvFileGo(path string) (map[string]string, error) {
	vars := map[string]string{}
	f, err := os.Open(path)
	if err != nil {
		return vars, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		idx := strings.Index(line, "=")
		if idx == -1 {
			continue
		}
		key := strings.TrimSpace(line[:idx])
		val := strings.TrimSpace(line[idx+1:])
		// Strip surrounding quotes
		if len(val) >= 2 {
			if (val[0] == '"' && val[len(val)-1] == '"') ||
				(val[0] == '\'' && val[len(val)-1] == '\'') {
				val = val[1 : len(val)-1]
			}
		}
		if key != "" {
			vars[key] = val
		}
	}
	return vars, scanner.Err()
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
