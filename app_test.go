package daggo_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/swetjen/daggo"
	"github.com/swetjen/daggo/dag"
)

type testScrapeOutput struct {
	HTML string
}

type testExtractInput struct {
	Scrape testScrapeOutput
}

type testExtractOutput struct {
	Title string
}

func TestOpenSyncsRegisteredJobsToDB(t *testing.T) {
	scrape := dag.Op[dag.NoInput, testScrapeOutput]("scrape", func(_ context.Context, _ dag.NoInput) (testScrapeOutput, error) {
		return testScrapeOutput{HTML: "<html><title>DAGGO</title></html>"}, nil
	})
	extract := dag.Op[testExtractInput, testExtractOutput]("extract_title", func(_ context.Context, in testExtractInput) (testExtractOutput, error) {
		return testExtractOutput{Title: in.Scrape.HTML}, nil
	})
	job := dag.NewJob("content_ingestion").
		WithDisplayName("Content Ingestion").
		Add(scrape, extract).
		MustBuild()

	cfg := daggo.DefaultConfig()
	cfg.Database.SQLite.Path = filepath.Join(t.TempDir(), "daggo.sqlite")
	cfg.Execution.Mode = dag.ExecutionModeInProcess

	app, err := daggo.Open(context.Background(), cfg, job)
	if err != nil {
		t.Fatalf("open app: %v", err)
	}
	t.Cleanup(func() {
		if err := app.Close(); err != nil {
			t.Fatalf("close app: %v", err)
		}
	})

	row, err := app.Deps().DB.JobGetByKey(context.Background(), job.Key)
	if err != nil {
		t.Fatalf("job not synced to db: %v", err)
	}
	if row.JobKey != job.Key {
		t.Fatalf("expected job key %q, got %q", job.Key, row.JobKey)
	}
}

func TestOpenRegistrySyncsRegisteredJobsToDB(t *testing.T) {
	scrape := dag.Op[dag.NoInput, testScrapeOutput]("scrape", func(_ context.Context, _ dag.NoInput) (testScrapeOutput, error) {
		return testScrapeOutput{HTML: "<html><title>DAGGO</title></html>"}, nil
	})
	job := dag.NewJob("registry_backed_job").Add(scrape).MustBuild()

	registry := dag.NewRegistry()
	if err := registry.Register(job); err != nil {
		t.Fatalf("register job: %v", err)
	}

	cfg := daggo.DefaultConfig()
	cfg.Database.SQLite.Path = filepath.Join(t.TempDir(), "daggo.sqlite")
	cfg.Execution.Mode = dag.ExecutionModeInProcess

	app, err := daggo.OpenRegistry(context.Background(), cfg, registry)
	if err != nil {
		t.Fatalf("open registry app: %v", err)
	}
	t.Cleanup(func() {
		if err := app.Close(); err != nil {
			t.Fatalf("close app: %v", err)
		}
	})

	row, err := app.Deps().DB.JobGetByKey(context.Background(), job.Key)
	if err != nil {
		t.Fatalf("job not synced to db: %v", err)
	}
	if row.JobKey != job.Key {
		t.Fatalf("expected job key %q, got %q", job.Key, row.JobKey)
	}
}
