package system

import (
	"context"

	"github.com/swetjen/daggo/deps"
	"github.com/swetjen/virtuous/rpc"
)

type Handlers struct {
	app *deps.Deps
}

func New(app *deps.Deps) *Handlers {
	return &Handlers{app: app}
}

type InfoGetResponse struct {
	Version string `json:"version"`
	Error   string `json:"error,omitempty"`
}

func (h *Handlers) InfoGet(_ context.Context) (InfoGetResponse, int) {
	version := ""
	if h != nil && h.app != nil {
		version = h.app.Version
	}
	return InfoGetResponse{Version: version}, rpc.StatusOK
}
