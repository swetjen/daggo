package main

import (
	"context"
	"log"

	api "daggo"
	"daggo/config"
	"daggo/db"
)

func main() {
	cfg := config.Load()
	queries, pool, err := db.Open(context.Background(), cfg.DatabaseURL)
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
