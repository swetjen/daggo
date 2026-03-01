package main

import (
	"context"
	"log"
	"strings"

	"github.com/swetjen/daggo"
	"github.com/swetjen/daggo/dag"
)

type ScrapePageOutput struct {
	URL  string
	HTML string
}

type ExtractTitleInput struct {
	Page ScrapePageOutput
}

type ExtractTitleOutput struct {
	Title string
}

type ExtractEntitiesInput struct {
	Page ScrapePageOutput
}

type ExtractEntitiesOutput struct {
	Entities []string
}

type ExtractLinksInput struct {
	Page ScrapePageOutput
}

type ExtractLinksOutput struct {
	Links []string
}

type UpdateIndexInput struct {
	Title    ExtractTitleOutput
	Entities ExtractEntitiesOutput
	Links    ExtractLinksOutput
}

type UpdateIndexOutput struct {
	RecordsUpserted int
}

func main() {
	cfg := daggo.DefaultConfig()
	cfg.Admin.Port = "8080"
	cfg.Database.SQLite.Path = "runtime/content-ingestion.sqlite"

	if err := daggo.Main(context.Background(), cfg, daggo.WithJobs(buildContentIngestionJob())); err != nil {
		log.Fatal(err)
	}
}

func buildContentIngestionJob() dag.JobDefinition {
	scrapePage := dag.Define[dag.NoInput, ScrapePageOutput]("scrape_page", func(_ context.Context, _ dag.NoInput) (ScrapePageOutput, error) {
		return ScrapePageOutput{
			URL:  "https://example.com/blog/daggo",
			HTML: "<html><head><title>DAGGO</title></head><body>Daggo extracts entities and links.</body></html>",
		}, nil
	}).WithDisplayName("Scrape Page")

	extractTitle := dag.Define[ExtractTitleInput, ExtractTitleOutput]("extract_title", func(_ context.Context, in ExtractTitleInput) (ExtractTitleOutput, error) {
		title := "Untitled"
		if strings.Contains(in.Page.HTML, "<title>DAGGO</title>") {
			title = "DAGGO"
		}
		return ExtractTitleOutput{Title: title}, nil
	}).WithDisplayName("Extract Title")

	extractEntities := dag.Define[ExtractEntitiesInput, ExtractEntitiesOutput]("extract_entities", func(_ context.Context, _ ExtractEntitiesInput) (ExtractEntitiesOutput, error) {
		return ExtractEntitiesOutput{Entities: []string{"Daggo", "SQLite", "Workflow"}}, nil
	}).WithDisplayName("Extract Entities")

	extractLinks := dag.Define[ExtractLinksInput, ExtractLinksOutput]("extract_links", func(_ context.Context, in ExtractLinksInput) (ExtractLinksOutput, error) {
		return ExtractLinksOutput{Links: []string{in.Page.URL, "https://example.com/docs/daggo"}}, nil
	}).WithDisplayName("Extract Links")

	updateIndex := dag.Define[UpdateIndexInput, UpdateIndexOutput]("update_search_index", func(_ context.Context, in UpdateIndexInput) (UpdateIndexOutput, error) {
		_ = in.Title
		_ = in.Entities
		_ = in.Links
		return UpdateIndexOutput{RecordsUpserted: 1}, nil
	}).WithDisplayName("Update Search Index")

	return dag.NewJob("content_ingestion").
		WithDisplayName("Content Ingestion").
		WithDescription("Scrape a page, fan out extraction work, then persist a consolidated record.").
		Add(scrapePage, extractTitle, extractEntities, extractLinks, updateIndex).
		AddSchedule(dag.ScheduleDefinition{
			CronExpr: "*/15 * * * *",
			Timezone: "UTC",
			Enabled:  true,
		}).
		MustBuild()
}
