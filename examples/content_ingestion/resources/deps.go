package resources

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/swetjen/daggo/resources/ollama"
	playwrightresource "github.com/swetjen/daggo/resources/playwright"
	"github.com/swetjen/daggo/resources/s3resource"
)

type Deps struct {
	Logger *slog.Logger
	HTTP   *http.Client

	// App-specific data access, for example your own *db.Queries or repository layer.
	CRUD any

	Playwright *playwrightresource.RemoteResource
	S3         *s3resource.Resource

	// Placeholder external clients commonly mounted into ops.
	Gemini  *http.Client
	OpenAPI *http.Client
	Ollama  *ollama.Resource
}

func NewDeps() Deps {
	httpClient := &http.Client{Timeout: 30 * time.Second}
	return Deps{
		Logger:     slog.Default(),
		HTTP:       httpClient,
		CRUD:       nil,
		Playwright: playwrightresource.NewFromEnv(),
		S3:         s3resource.NewFromEnv(),
		Gemini:     httpClient,
		OpenAPI:    httpClient,
		Ollama:     ollama.NewFromEnv(),
	}
}
