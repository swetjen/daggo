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
	myJobs := jobs.ContentIngestionJob(myOps)

	if err := daggo.Run(context.Background(), cfg, myJobs); err != nil {
		log.Fatal(err)
	}
}
