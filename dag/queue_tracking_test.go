package dag

import (
	"testing"
	"time"

	"github.com/swetjen/daggo/db"
)

func TestAggregateQueueItemLifecycle(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 3, 15, 1, 0, 0, 0, time.UTC)
	plus := func(minutes int) string {
		return base.Add(time.Duration(minutes) * time.Minute).Format(time.RFC3339Nano)
	}

	tests := []struct {
		name              string
		rows              []db.QueueItemRunGetManyByQueueItemIDJoinedRunsRow
		wantStatus        string
		wantStartedAt     string
		wantCompletedAt   string
		wantErrorContains string
	}{
		{
			name: "all queued stays queued",
			rows: []db.QueueItemRunGetManyByQueueItemIDJoinedRunsRow{
				{RunStatus: "queued"},
				{RunStatus: "queued"},
			},
			wantStatus: "queued",
		},
		{
			name: "mixed queued and running becomes running with earliest start",
			rows: []db.QueueItemRunGetManyByQueueItemIDJoinedRunsRow{
				{RunStatus: "running", StartedAt: plus(5)},
				{RunStatus: "queued"},
				{RunStatus: "running", StartedAt: plus(2)},
			},
			wantStatus:    "running",
			wantStartedAt: plus(2),
		},
		{
			name: "all success becomes complete with latest completion",
			rows: []db.QueueItemRunGetManyByQueueItemIDJoinedRunsRow{
				{RunStatus: "success", StartedAt: plus(1), CompletedAt: plus(3)},
				{RunStatus: "success", StartedAt: plus(2), CompletedAt: plus(7), RunErrorMessage: "ignored"},
			},
			wantStatus:      "complete",
			wantStartedAt:   plus(1),
			wantCompletedAt: plus(7),
		},
		{
			name: "failed or canceled terminal mix becomes failed and preserves first error",
			rows: []db.QueueItemRunGetManyByQueueItemIDJoinedRunsRow{
				{RunStatus: "success", StartedAt: plus(1), CompletedAt: plus(2)},
				{RunStatus: "failed", StartedAt: plus(3), CompletedAt: plus(8), RunErrorMessage: "boom"},
				{RunStatus: "canceled", StartedAt: plus(4), CompletedAt: plus(9), RunErrorMessage: "ignored later"},
			},
			wantStatus:        "failed",
			wantStartedAt:     plus(1),
			wantCompletedAt:   plus(9),
			wantErrorContains: "boom",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			gotStatus, gotStartedAt, gotCompletedAt, gotError := aggregateQueueItemLifecycle(test.rows)
			if gotStatus != test.wantStatus {
				t.Fatalf("expected status %q, got %q", test.wantStatus, gotStatus)
			}
			if gotStartedAt != test.wantStartedAt {
				t.Fatalf("expected started_at %q, got %q", test.wantStartedAt, gotStartedAt)
			}
			if gotCompletedAt != test.wantCompletedAt {
				t.Fatalf("expected completed_at %q, got %q", test.wantCompletedAt, gotCompletedAt)
			}
			if test.wantErrorContains == "" {
				if gotError != "" {
					t.Fatalf("expected empty error, got %q", gotError)
				}
				return
			}
			if gotError != test.wantErrorContains {
				t.Fatalf("expected error %q, got %q", test.wantErrorContains, gotError)
			}
		})
	}
}
