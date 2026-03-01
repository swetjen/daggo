package main

import (
	"context"
	"log"

	"github.com/swetjen/daggo"
	"github.com/swetjen/daggo/examples/content_ingestion/jobs"
	"github.com/swetjen/daggo/examples/content_ingestion/ops"
	"github.com/swetjen/daggo/examples/content_ingestion/resources"
)

func main() {
	cfg := daggo.DefaultConfig()
	cfg.Admin.Port = "8080"
	cfg.Database.SQLite.Path = "runtime/content-ingestion.sqlite"

	deps := resources.NewDeps()
	myOps := ops.NewMyOps(deps)

	if err := daggo.Main(context.Background(), cfg, daggo.WithJobs(jobs.ContentIngestionJob(myOps))); err != nil {
		log.Fatal(err)
	}
}
