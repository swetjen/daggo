package runs

import (
	"context"
	"testing"

	"daggo/deps"
	"github.com/swetjen/virtuous/rpc"
)

func TestRunTerminateRequiresID(t *testing.T) {
	t.Parallel()

	h := New(&deps.Deps{})
	resp, status := h.RunTerminate(context.Background(), RunTerminateRequest{})
	if status != rpc.StatusInvalid {
		t.Fatalf("expected invalid status, got %d", status)
	}
	if resp.Error != "id is required" {
		t.Fatalf("expected id required error, got %q", resp.Error)
	}
}
