package playwrightresource

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	playwright "github.com/playwright-community/playwright-go"
)

const (
	wsProtocolPlaywright = "playwright"
	wsProtocolCDP        = "cdp"
)

type RemoteResource struct {
	WSURL                  string
	WSProtocol             string
	CDPURL                 string
	NavigationTimeout      time.Duration
	ScrollStep             int
	ScrollDelay            time.Duration
	MaxScrolls             int
	RequireExistingContext bool
}

func NewFromEnv() *RemoteResource {
	return &RemoteResource{
		WSURL:                  envOr("PLAYWRIGHT_WS_URL", "ws://localhost:3000/"),
		WSProtocol:             envOr("PLAYWRIGHT_WS_PROTOCOL", wsProtocolPlaywright),
		CDPURL:                 envOr("PLAYWRIGHT_CDP_URL", "http://localhost:9222"),
		NavigationTimeout:      60 * time.Second,
		ScrollStep:             200,
		ScrollDelay:            300 * time.Millisecond,
		MaxScrolls:             10,
		RequireExistingContext: envBool("PLAYWRIGHT_REQUIRE_EXISTING_CONTEXT", false),
	}
}

func (r *RemoteResource) Scrape(ctx context.Context, targetURL string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	pw, err := playwright.Run()
	if err != nil {
		return "", fmt.Errorf("playwright: start runtime: %w", err)
	}
	defer func() {
		_ = pw.Stop()
	}()

	protocol := r.wsProtocol()
	var browser playwright.Browser
	if protocol == wsProtocolCDP {
		browser, err = pw.Chromium.ConnectOverCDP(strings.TrimSpace(r.WSURL))
		if err != nil {
			return "", fmt.Errorf("playwright: connect websocket as cdp: %w", err)
		}
	} else {
		browser, err = pw.Chromium.Connect(strings.TrimSpace(r.WSURL))
		if err != nil {
			return "", fmt.Errorf("playwright: connect websocket: %w", err)
		}
	}
	defer func() {
		_ = browser.Close()
	}()

	var page playwright.Page
	if protocol == wsProtocolCDP {
		page, err = r.newPageFromBrowser(browser)
	} else {
		page, err = browser.NewPage()
	}
	if err != nil {
		return "", err
	}
	defer func() {
		_ = page.Close()
	}()

	timeoutMillis := float64(r.navigationTimeout().Milliseconds())
	_, err = page.Goto(targetURL, playwright.PageGotoOptions{
		WaitUntil: playwright.WaitUntilStateDomcontentloaded,
		Timeout:   playwright.Float(timeoutMillis),
	})
	if err != nil {
		return "", fmt.Errorf("playwright: goto %s: %w", targetURL, err)
	}

	html, err := page.Content()
	if err != nil {
		return "", fmt.Errorf("playwright: read page content: %w", err)
	}
	return html, nil
}

func (r *RemoteResource) ScrapeHeaded(ctx context.Context, targetURL string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	pw, err := playwright.Run()
	if err != nil {
		return "", fmt.Errorf("playwright: start runtime: %w", err)
	}
	defer func() {
		_ = pw.Stop()
	}()

	browser, err := pw.Chromium.ConnectOverCDP(strings.TrimSpace(r.CDPURL))
	if err != nil {
		return "", fmt.Errorf("playwright: connect over cdp: %w", err)
	}
	defer func() {
		_ = browser.Close()
	}()

	page, err := r.newPageFromBrowser(browser)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = page.Close()
	}()

	timeoutMillis := float64(r.navigationTimeout().Milliseconds())
	_, err = page.Goto(targetURL, playwright.PageGotoOptions{
		WaitUntil: playwright.WaitUntilStateLoad,
		Timeout:   playwright.Float(timeoutMillis),
	})
	if err != nil {
		if isTimeoutError(err) {
			html, contentErr := page.Content()
			if contentErr != nil {
				return "", fmt.Errorf("playwright: timeout and failed to read content: %w", contentErr)
			}
			return html, nil
		}
		return "", fmt.Errorf("playwright: goto %s: %w", targetURL, err)
	}

	time.Sleep(2 * time.Second)

	html := ""
	for i := 0; i < r.maxScrolls(); i++ {
		if html == "" {
			content, err := page.Content()
			if err == nil {
				html = content
			}
		}
		if page.IsClosed() {
			break
		}

		atBottomRaw, err := page.Evaluate(fmt.Sprintf(`
			() => {
				const scrollTop = window.scrollY;
				const viewportHeight = window.innerHeight;
				const totalHeight = document.documentElement.scrollHeight;
				window.scrollBy(0, %d);
				return (scrollTop + viewportHeight) >= (totalHeight - 5);
			}
		`, r.scrollStep()))
		if err != nil {
			break
		}
		atBottom, _ := atBottomRaw.(bool)
		if atBottom {
			break
		}
		time.Sleep(r.scrollDelayOrDefault())
	}

	time.Sleep(1 * time.Second)
	if html == "" {
		content, err := page.Content()
		if err != nil {
			return "", fmt.Errorf("playwright: read page content: %w", err)
		}
		html = content
	}
	return html, nil
}

func (r *RemoteResource) newPageFromBrowser(browser playwright.Browser) (playwright.Page, error) {
	contexts := browser.Contexts()
	if len(contexts) > 0 {
		page, err := contexts[0].NewPage()
		if err != nil {
			return nil, fmt.Errorf("playwright: create cdp page: %w", err)
		}
		return page, nil
	}
	if r.requireExistingContext() {
		return nil, errors.New("playwright: no existing browser context available")
	}
	browserContext, err := browser.NewContext()
	if err != nil {
		return nil, fmt.Errorf("playwright: create browser context: %w", err)
	}
	page, err := browserContext.NewPage()
	if err != nil {
		return nil, fmt.Errorf("playwright: create cdp page: %w", err)
	}
	return page, nil
}

func (r *RemoteResource) navigationTimeout() time.Duration {
	if r == nil || r.NavigationTimeout <= 0 {
		return 60 * time.Second
	}
	return r.NavigationTimeout
}

func (r *RemoteResource) scrollStep() int {
	if r == nil || r.ScrollStep <= 0 {
		return 200
	}
	return r.ScrollStep
}

func (r *RemoteResource) scrollDelayOrDefault() time.Duration {
	if r == nil || r.ScrollDelay <= 0 {
		return 300 * time.Millisecond
	}
	return r.ScrollDelay
}

func (r *RemoteResource) maxScrolls() int {
	if r == nil || r.MaxScrolls <= 0 {
		return 10
	}
	return r.MaxScrolls
}

func (r *RemoteResource) wsProtocol() string {
	protocol := strings.ToLower(strings.TrimSpace(r.WSProtocol))
	switch protocol {
	case wsProtocolCDP:
		return wsProtocolCDP
	default:
		return wsProtocolPlaywright
	}
}

func (r *RemoteResource) requireExistingContext() bool {
	if r == nil {
		return false
	}
	return r.RequireExistingContext
}

func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "timeout")
}

func envOr(name string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}

func envBool(name string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	switch value {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}
