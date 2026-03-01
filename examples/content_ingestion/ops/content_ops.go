package ops

import (
	"context"
	"strings"

	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/examples/content_ingestion/resources"
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

func (o *MyOps) ScrapePageOp(_ context.Context, _ dag.NoInput) (ScrapePageOutput, error) {
	if o.deps.Logger != nil {
		o.deps.Logger.Info("scraping page")
	}
	return ScrapePageOutput{
		URL:  "https://example.com/blog/daggo",
		HTML: "<html><head><title>DAGGO</title></head><body>Daggo extracts entities and links.</body></html>",
	}, nil
}

func (o *MyOps) ExtractTitleOp(_ context.Context, in ExtractTitleInput) (ExtractTitleOutput, error) {
	_ = o
	title := "Untitled"
	if strings.Contains(in.Page.HTML, "<title>DAGGO</title>") {
		title = "DAGGO"
	}
	return ExtractTitleOutput{Title: title}, nil
}

func (o *MyOps) ExtractEntitiesOp(_ context.Context, _ ExtractEntitiesInput) (ExtractEntitiesOutput, error) {
	_ = o
	return ExtractEntitiesOutput{Entities: []string{"Daggo", "SQLite", "Workflow"}}, nil
}

func (o *MyOps) ExtractLinksOp(_ context.Context, in ExtractLinksInput) (ExtractLinksOutput, error) {
	_ = o
	return ExtractLinksOutput{Links: []string{in.Page.URL, "https://example.com/docs/daggo"}}, nil
}

func (o *MyOps) UpsertIndexOp(_ context.Context, in UpsertIndexInput) (UpsertIndexOutput, error) {
	_ = in.Title
	_ = in.Entities
	_ = in.Links
	if o.deps.Logger != nil {
		o.deps.Logger.Info("upserting search index")
	}
	return UpsertIndexOutput{RecordsUpserted: 1}, nil
}
