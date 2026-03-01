package resources

import (
	"context"
	"net/http"

	"github.com/openai/openai-go/v3"
	db "github.com/swetjen/daggo/examples/content_ingestion/db"
	"github.com/swetjen/daggo/resources/ollama"
	playwrightresource "github.com/swetjen/daggo/resources/playwright"
	"github.com/swetjen/daggo/resources/s3resource"
	"google.golang.org/genai"
)

type ScrapeResult struct {
	Body       string
	StatusCode int
}

type Deps struct {
	CRUD *db.Queries

	Playwright *playwrightresource.RemoteResource
	S3         *s3resource.Resource

	Gemini *genai.Client
	OpenAI *openai.Client
	Ollama *ollama.Resource

	Scraper func(ctx context.Context, targetURL string) (ScrapeResult, error)
}

func NewDeps() Deps {
	openAI := openai.NewClient()
	return Deps{
		CRUD:       &db.Queries{},
		Playwright: playwrightresource.NewFromEnv(),
		S3:         s3resource.NewFromEnv(),
		Gemini:     nil,
		OpenAI:     &openAI,
		Ollama:     ollama.NewFromEnv(),
		Scraper: func(ctx context.Context, targetURL string) (ScrapeResult, error) {
			if err := ctx.Err(); err != nil {
				return ScrapeResult{}, err
			}
			return ScrapeResult{
				Body:       "<html><head><title>DAGGO</title></head><body>Daggo extracts entities and links.</body></html>",
				StatusCode: http.StatusOK,
			}, nil
		},
	}
}
