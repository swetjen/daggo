package ollama

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type QueryOptions struct {
	Model             string
	RetryDelays       []time.Duration
	KeepAlive         string
	TreatEmptyAsError bool
	AuthHeader        string
}

type Resource struct {
	BaseURL          string
	APIKey           string
	AuthHeader       string
	Model            string
	ImageModel       string
	Timeout          time.Duration
	HTTPClient       *http.Client
	TextRetryDelays  []time.Duration
	ImageRetryDelays []time.Duration
}

func NewFromEnv() *Resource {
	return &Resource{
		BaseURL:          envOr("OLLAMA_URL", "http://localhost:11434"),
		APIKey:           strings.TrimSpace(os.Getenv("OLLAMA_API_KEY")),
		AuthHeader:       strings.TrimSpace(os.Getenv("OLLAMA_AUTH_HEADER")),
		Model:            "nemotron-3-nano:latest",
		ImageModel:       "qwen3-vl:8b",
		Timeout:          600 * time.Second,
		TextRetryDelays:  defaultTextRetryDelays(),
		ImageRetryDelays: defaultImageRetryDelays(),
	}
}

func (r *Resource) Query(ctx context.Context, messages []Message, model string) (string, error) {
	return r.QueryWithOptions(ctx, messages, QueryOptions{Model: model})
}

func (r *Resource) QueryWithOptions(ctx context.Context, messages []Message, opts QueryOptions) (string, error) {
	selectedModel := strings.TrimSpace(opts.Model)
	if selectedModel == "" {
		selectedModel = strings.TrimSpace(r.Model)
	}
	if selectedModel == "" {
		return "", errors.New("ollama: model is required")
	}
	retryDelays := opts.RetryDelays
	if len(retryDelays) == 0 {
		retryDelays = r.textRetryDelaysOrDefault()
	}
	return r.queryWithRetry(ctx, selectedModel, messages, retryDelays, opts.KeepAlive, opts.TreatEmptyAsError, opts.AuthHeader)
}

func (r *Resource) QueryImage(ctx context.Context, messages []Message, model string) (string, error) {
	return r.QueryImageWithOptions(ctx, messages, QueryOptions{Model: model})
}

func (r *Resource) QueryImageWithOptions(ctx context.Context, messages []Message, opts QueryOptions) (string, error) {
	selectedModel := strings.TrimSpace(opts.Model)
	if selectedModel == "" {
		selectedModel = strings.TrimSpace(r.ImageModel)
	}
	if selectedModel == "" {
		return "", errors.New("ollama: image model is required")
	}

	retryDelays := opts.RetryDelays
	if len(retryDelays) == 0 {
		retryDelays = r.imageRetryDelaysOrDefault()
	}

	response, err := r.queryWithRetry(ctx, selectedModel, messages, retryDelays, opts.KeepAlive, opts.TreatEmptyAsError, opts.AuthHeader)
	if err == nil {
		return response, nil
	}

	keepAlive := strings.TrimSpace(opts.KeepAlive)
	if keepAlive == "" {
		keepAlive = "10s"
	}
	return r.queryOnce(ctx, selectedModel, messages, keepAlive, opts.TreatEmptyAsError, opts.AuthHeader)
}

func (r *Resource) queryWithRetry(ctx context.Context, model string, messages []Message, retryDelays []time.Duration, keepAlive string, treatEmptyAsError bool, authHeaderOverride string) (string, error) {
	var lastErr error
	for _, delay := range retryDelays {
		response, err := r.queryOnce(ctx, model, messages, keepAlive, treatEmptyAsError, authHeaderOverride)
		if err == nil {
			return response, nil
		}
		lastErr = err
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", ctx.Err()
		case <-timer.C:
		}
	}
	if lastErr == nil {
		lastErr = errors.New("ollama: retry failed with unknown error")
	}
	return "", lastErr
}

func (r *Resource) queryOnce(ctx context.Context, model string, messages []Message, keepAlive string, treatEmptyAsError bool, authHeaderOverride string) (string, error) {
	base := strings.TrimRight(strings.TrimSpace(r.BaseURL), "/")
	if base == "" {
		return "", errors.New("ollama: base URL is required")
	}

	payload := map[string]any{
		"model":    model,
		"messages": messages,
		"stream":   false,
	}
	if strings.TrimSpace(keepAlive) != "" {
		payload["keep_alive"] = keepAlive
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("ollama: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/api/chat", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("ollama: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if authHeader := r.authorizationHeader(authHeaderOverride); authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}

	resp, err := r.httpClient().Do(req)
	if err != nil {
		return "", fmt.Errorf("ollama: execute request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("ollama: read response: %w", err)
	}
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("ollama: request failed (%d): %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}

	var parsed chatResponse
	if err := json.Unmarshal(responseBody, &parsed); err != nil {
		return "", fmt.Errorf("ollama: decode response: %w", err)
	}

	content := strings.TrimSpace(parsed.Message.Content)
	if content == "" && treatEmptyAsError {
		return "", errors.New("ollama: response content was empty")
	}
	return content, nil
}

func (r *Resource) authorizationHeader(override string) string {
	if header := strings.TrimSpace(override); header != "" {
		return header
	}
	if header := strings.TrimSpace(r.AuthHeader); header != "" {
		return header
	}
	if key := strings.TrimSpace(r.APIKey); key != "" {
		return "Bearer " + key
	}
	return ""
}

func (r *Resource) textRetryDelaysOrDefault() []time.Duration {
	if len(r.TextRetryDelays) == 0 {
		return defaultTextRetryDelays()
	}
	return r.TextRetryDelays
}

func (r *Resource) imageRetryDelaysOrDefault() []time.Duration {
	if len(r.ImageRetryDelays) == 0 {
		return defaultImageRetryDelays()
	}
	return r.ImageRetryDelays
}

func defaultTextRetryDelays() []time.Duration {
	return []time.Duration{1 * time.Second, 2 * time.Second, 3 * time.Second, 4 * time.Second, 5 * time.Second, 6 * time.Second, 7 * time.Second, 8 * time.Second, 9 * time.Second}
}

func defaultImageRetryDelays() []time.Duration {
	return []time.Duration{10 * time.Second, 11 * time.Second, 12 * time.Second, 13 * time.Second, 14 * time.Second}
}

func (r *Resource) httpClient() *http.Client {
	if r.HTTPClient != nil {
		return r.HTTPClient
	}
	timeout := r.Timeout
	if timeout <= 0 {
		timeout = 600 * time.Second
	}
	return &http.Client{Timeout: timeout}
}

type chatResponse struct {
	Message struct {
		Content string `json:"content"`
	} `json:"message"`
}

func envOr(name string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}
