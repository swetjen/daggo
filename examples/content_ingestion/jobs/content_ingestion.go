package jobs

import (
	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/examples/content_ingestion/ops"
)

func ContentIngestionJob(myOps *ops.MyOps) dag.JobDefinition {
	scrapePage := dag.Op[dag.NoInput, ops.ScrapePageOutput]("scrape_page", myOps.ScrapePageOp).
		WithDisplayName("Scrape Page")

	extractTitle := dag.Op[ops.ExtractTitleInput, ops.ExtractTitleOutput]("extract_title", myOps.ExtractTitleOp).
		WithDisplayName("Extract Title")

	extractEntities := dag.Op[ops.ExtractEntitiesInput, ops.ExtractEntitiesOutput]("extract_entities", myOps.ExtractEntitiesOp).
		WithDisplayName("Extract Entities")

	extractLinks := dag.Op[ops.ExtractLinksInput, ops.ExtractLinksOutput]("extract_links", myOps.ExtractLinksOp).
		WithDisplayName("Extract Links")

	upsertIndex := dag.Op[ops.UpsertIndexInput, ops.UpsertIndexOutput]("upsert_index", myOps.UpsertIndexOp).
		WithDisplayName("Upsert Index")

	return dag.NewJob("content_ingestion").
		WithDisplayName("Content Ingestion").
		WithDescription("Scrape a page, fan out extraction work, then persist a consolidated record.").
		Add(scrapePage, extractTitle, extractEntities, extractLinks, upsertIndex).
		AddSchedule(dag.ScheduleDefinition{
			CronExpr: "*/15 * * * *",
			Timezone: "UTC",
			Enabled:  true,
		}).
		MustBuild()
}
