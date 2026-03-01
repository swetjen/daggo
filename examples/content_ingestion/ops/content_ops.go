package ops

import (
	"context"
	"strings"

	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/examples/content_ingestion/resources"
)

type ScrapePageOutput struct {
	URL        string
	Body       string
	StatusCode int
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

type UpsertIndexInput struct {
	Title    ExtractTitleOutput
	Entities ExtractEntitiesOutput
	Links    ExtractLinksOutput
}

type UpsertIndexOutput struct {
	RecordsUpserted int
}

type MyOps struct {
	deps resources.Deps
}

func NewMyOps(deps resources.Deps) *MyOps {
	return &MyOps{deps: deps}
}

func (o *MyOps) ScrapePageOp(ctx context.Context, input dag.NoInput) (ScrapePageOutput, error) {
	if err := ctx.Err(); err != nil {
		return ScrapePageOutput{}, err
	}

	// Run some processing on the input.
	result, err := o.deps.Scraper(ctx, o.scrapeTargetURL(input))
	if err != nil {
		return ScrapePageOutput{}, err
	}

	return ScrapePageOutput{
		URL:        "https://example.com/blog/daggo",
		Body:       result.Body,
		StatusCode: result.StatusCode,
	}, nil
}

func (o *MyOps) scrapeTargetURL(dag.NoInput) string {
	return "https://example.com/blog/daggo"
}

func (o *MyOps) ExtractTitleOp(ctx context.Context, in ExtractTitleInput) (ExtractTitleOutput, error) {
	if err := ctx.Err(); err != nil {
		return ExtractTitleOutput{}, err
	}
	title := "Untitled"
	if strings.Contains(in.Page.Body, "<title>DAGGO</title>") {
		title = "DAGGO"
	}
	return ExtractTitleOutput{Title: title}, nil
}

func (o *MyOps) ExtractEntitiesOp(ctx context.Context, in ExtractEntitiesInput) (ExtractEntitiesOutput, error) {
	if err := ctx.Err(); err != nil {
		return ExtractEntitiesOutput{}, err
	}
	entities := []string{"Daggo", "Workflow"}
	if strings.Contains(in.Page.Body, "SQLite") {
		entities = append(entities, "SQLite")
	}
	return ExtractEntitiesOutput{Entities: entities}, nil
}

func (o *MyOps) ExtractLinksOp(ctx context.Context, in ExtractLinksInput) (ExtractLinksOutput, error) {
	if err := ctx.Err(); err != nil {
		return ExtractLinksOutput{}, err
	}
	return ExtractLinksOutput{Links: []string{in.Page.URL, "https://example.com/docs/daggo"}}, nil
}

func (o *MyOps) UpsertIndexOp(ctx context.Context, in UpsertIndexInput) (UpsertIndexOutput, error) {
	if err := ctx.Err(); err != nil {
		return UpsertIndexOutput{}, err
	}

	recordsUpserted := len(in.Entities.Entities) + len(in.Links.Links)
	if in.Title.Title != "" {
		recordsUpserted++
	}
	if recordsUpserted == 0 && o.deps.CRUD != nil {
		recordsUpserted = 1
	}
	return UpsertIndexOutput{RecordsUpserted: recordsUpserted}, nil
}
