package handlers

import (
	"github.com/swetjen/daggo/deps"
	"github.com/swetjen/daggo/handlers/backfills"
	"github.com/swetjen/daggo/handlers/jobs"
	"github.com/swetjen/daggo/handlers/runs"
	"github.com/swetjen/daggo/handlers/schedules"
)

type Handlers struct {
	Backfills *backfills.Handlers
	Jobs      *jobs.Handlers
	Runs      *runs.Handlers
	Schedules *schedules.Handlers
}

func New(app *deps.Deps) *Handlers {
	return &Handlers{
		Backfills: backfills.New(app),
		Jobs:      jobs.New(app),
		Runs:      runs.New(app),
		Schedules: schedules.New(app),
	}
}
