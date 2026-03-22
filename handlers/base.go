package handlers

import (
	"github.com/swetjen/daggo/deps"
	"github.com/swetjen/daggo/handlers/jobs"
	queuehandlers "github.com/swetjen/daggo/handlers/queues"
	"github.com/swetjen/daggo/handlers/runs"
	"github.com/swetjen/daggo/handlers/schedules"
	"github.com/swetjen/daggo/handlers/system"
)

type Handlers struct {
	Jobs      *jobs.Handlers
	Queues    *queuehandlers.Handlers
	Runs      *runs.Handlers
	Schedules *schedules.Handlers
	System    *system.Handlers
}

func New(app *deps.Deps) *Handlers {
	return &Handlers{
		Jobs:      jobs.New(app),
		Queues:    queuehandlers.New(app),
		Runs:      runs.New(app),
		Schedules: schedules.New(app),
		System:    system.New(app),
	}
}
