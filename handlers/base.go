package handlers

import (
	"daggo/deps"
	"daggo/handlers/jobs"
	"daggo/handlers/runs"
	"daggo/handlers/schedules"
)

type Handlers struct {
	Jobs      *jobs.Handlers
	Runs      *runs.Handlers
	Schedules *schedules.Handlers
}

func New(app *deps.Deps) *Handlers {
	return &Handlers{
		Jobs:      jobs.New(app),
		Runs:      runs.New(app),
		Schedules: schedules.New(app),
	}
}
