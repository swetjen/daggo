package main

import (
	"log"

	api "github.com/swetjen/daggo"
	"github.com/swetjen/daggo/db"
)

func main() {
	cfg := api.DefaultConfig()
	cfg.Admin.SecretKey = "sdk-generation-placeholder"
	queries, pool, err := db.NewTest()
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	router, err := api.BuildRouter(cfg, queries, pool)
	if err != nil {
		log.Fatal(err)
	}

	if err := api.WriteFrontendClient(router); err != nil {
		log.Fatal(err)
	}
	if err := router.WriteClientTSFile("client.gen.ts"); err != nil {
		log.Fatal(err)
	}
	if err := router.WriteClientPYFile("client.gen.py"); err != nil {
		log.Fatal(err)
	}
}
