package main

import (
	"context"
	"log"
	"os"

	"github.com/swetjen/daggo"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "worker" {
		os.Args[1] = "daggo-worker"
	}
	cfg := daggo.LoadConfigFromEnv()
	if err := daggo.Run(context.Background(), cfg); err != nil {
		log.Fatal(err)
	}
}
