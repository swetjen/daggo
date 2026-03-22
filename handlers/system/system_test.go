package system

import (
	"context"
	"testing"

	"github.com/swetjen/daggo/deps"
	"github.com/swetjen/virtuous/rpc"
)

func TestInfoGetReturnsVersion(t *testing.T) {
	t.Parallel()

	handler := New(&deps.Deps{Version: "v0.5.0"})
	resp, status := handler.InfoGet(context.Background())
	if status != rpc.StatusOK {
		t.Fatalf("expected ok status, got %d", status)
	}
	if resp.Version != "v0.5.0" {
		t.Fatalf("expected version v0.5.0, got %q", resp.Version)
	}
}
