package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/swetjen/daggo/db"
)

type exSourceOutput struct {
	Value int `json:"value"`
}

type exConsumerInput struct {
	Source exSourceOutput `json:"source"`
}

type exConsumerOutput struct {
	Doubled int `json:"doubled"`
}

type exTransformInput struct {
	Source exSourceOutput `json:"source"`
}

type exTransformOutput struct {
	Transformed int `json:"transformed"`
}

type exPublishInput struct {
	Transform exTransformOutput `json:"transform"`
}

type exPublishOutput struct {
	Done bool `json:"done"`
}

type exPanicOutput struct {
	OK bool `json:"ok"`
}

type exMarshalOutput struct {
	Bad func() `json:"bad"`
}

type exHydrateProviderOutput struct {
	Value int `json:"value"`
}

type exHydrateConsumerInput struct {
	Provider exHydrateProviderOutput `json:"provider"`
}

type exHydrateConsumerOutput struct {
	Value int `json:"value"`
}

type exParallelAOutput struct {
	Value int `json:"value"`
}

type exParallelBOutput struct {
	Value int `json:"value"`
}

type exParallelJoinInput struct {
	A exParallelAOutput `json:"a"`
	B exParallelBOutput `json:"b"`
}

type exParallelJoinOutput struct {
	Done bool `json:"done"`
}

func TestExecutorExecuteRun_HappyPath(t *testing.T) {
	source := Define[NoInput, exSourceOutput]("source", func(_ context.Context, _ NoInput) (exSourceOutput, error) {
		return exSourceOutput{Value: 21}, nil
	})
	consumer := Define[exConsumerInput, exConsumerOutput]("consumer", func(_ context.Context, in exConsumerInput) (exConsumerOutput, error) {
		return exConsumerOutput{Doubled: in.Source.Value * 2}, nil
	})
	job := NewJob("executor_happy").Add(source, consumer).MustBuild()

	ctx, queries, executor := newExecutorTestHarness(t, job)
	run := createRunWithPendingSteps(t, ctx, queries, job.Key, 0, "", "{}")

	if err := executor.executeRun(ctx, run.ID); err != nil {
		t.Fatalf("execute run: %v", err)
	}

	runRow, err := queries.RunGetByID(ctx, run.ID)
	if err != nil {
		t.Fatalf("load run: %v", err)
	}
	if runRow.Status != "success" {
		t.Fatalf("expected run success, got %q", runRow.Status)
	}
	if runRow.ErrorMessage != "" {
		t.Fatalf("expected empty run error message, got %q", runRow.ErrorMessage)
	}
	if strings.TrimSpace(runRow.StartedAt) == "" || strings.TrimSpace(runRow.CompletedAt) == "" {
		t.Fatalf("expected run timestamps to be set")
	}

	steps, err := queries.RunStepGetManyByRunID(ctx, run.ID)
	if err != nil {
		t.Fatalf("load run steps: %v", err)
	}
	sourceStep := mustFindRunStep(t, steps, "source")
	consumerStep := mustFindRunStep(t, steps, "consumer")
	if sourceStep.Status != "success" {
		t.Fatalf("expected source step success, got %q", sourceStep.Status)
	}
	if consumerStep.Status != "success" {
		t.Fatalf("expected consumer step success, got %q", consumerStep.Status)
	}

	var consumerOutput exConsumerOutput
	if err := json.Unmarshal([]byte(consumerStep.OutputJson), &consumerOutput); err != nil {
		t.Fatalf("decode consumer output: %v", err)
	}
	if consumerOutput.Doubled != 42 {
		t.Fatalf("expected doubled output 42, got %d", consumerOutput.Doubled)
	}

	events := mustLoadRunEvents(t, ctx, queries, run.ID)
	assertRunEventSequence(t, events,
		[]string{"run_started", "step_started", "step_succeeded", "step_started", "step_succeeded", "run_completed"},
		[]string{"", "source", "source", "consumer", "consumer", ""},
	)
	assertRunCompletedEventStatus(t, events, "success")
}

func TestExecutorExecuteRun_StepFailureSkipsDownstream(t *testing.T) {
	source := Define[NoInput, exSourceOutput]("source", func(_ context.Context, _ NoInput) (exSourceOutput, error) {
		return exSourceOutput{Value: 7}, nil
	})
	transform := Define[exTransformInput, exTransformOutput]("transform", func(_ context.Context, _ exTransformInput) (exTransformOutput, error) {
		return exTransformOutput{}, fmt.Errorf("transform exploded")
	})
	publish := Define[exPublishInput, exPublishOutput]("publish", func(_ context.Context, _ exPublishInput) (exPublishOutput, error) {
		return exPublishOutput{Done: true}, nil
	})
	job := NewJob("executor_failure").Add(source, transform, publish).MustBuild()

	ctx, queries, executor := newExecutorTestHarness(t, job)
	run := createRunWithPendingSteps(t, ctx, queries, job.Key, 0, "", "{}")

	if err := executor.executeRun(ctx, run.ID); err != nil {
		t.Fatalf("execute run: %v", err)
	}

	runRow, err := queries.RunGetByID(ctx, run.ID)
	if err != nil {
		t.Fatalf("load run: %v", err)
	}
	if runRow.Status != "failed" {
		t.Fatalf("expected run failed, got %q", runRow.Status)
	}
	if !strings.Contains(runRow.ErrorMessage, "transform exploded") {
		t.Fatalf("expected failure message to mention transform error, got %q", runRow.ErrorMessage)
	}

	steps, err := queries.RunStepGetManyByRunID(ctx, run.ID)
	if err != nil {
		t.Fatalf("load run steps: %v", err)
	}
	sourceStep := mustFindRunStep(t, steps, "source")
	transformStep := mustFindRunStep(t, steps, "transform")
	publishStep := mustFindRunStep(t, steps, "publish")

	if sourceStep.Status != "success" {
		t.Fatalf("expected source step success, got %q", sourceStep.Status)
	}
	if transformStep.Status != "failed" {
		t.Fatalf("expected transform step failed, got %q", transformStep.Status)
	}
	if !strings.Contains(transformStep.ErrorMessage, "transform exploded") {
		t.Fatalf("expected transform step failure message, got %q", transformStep.ErrorMessage)
	}
	if publishStep.Status != "skipped" {
		t.Fatalf("expected publish step skipped, got %q", publishStep.Status)
	}
	if !strings.Contains(publishStep.ErrorMessage, "dependency output unavailable") {
		t.Fatalf("expected publish skipped reason, got %q", publishStep.ErrorMessage)
	}

	events := mustLoadRunEvents(t, ctx, queries, run.ID)
	assertRunEventSequence(t, events,
		[]string{"run_started", "step_started", "step_succeeded", "step_started", "step_failed", "step_skipped", "run_completed"},
		[]string{"", "source", "source", "transform", "transform", "publish", ""},
	)
	assertRunCompletedEventStatus(t, events, "failed")
}

func TestExecutorExecuteRun_Resilience(t *testing.T) {
	t.Run("panic_recovery_marks_run_failed", func(t *testing.T) {
		panicStep := Define[NoInput, exPanicOutput]("panic_step", func(_ context.Context, _ NoInput) (exPanicOutput, error) {
			panic("kaboom")
		})
		job := NewJob("executor_panic").Add(panicStep).MustBuild()

		ctx, queries, executor := newExecutorTestHarness(t, job)
		run := createRunWithPendingSteps(t, ctx, queries, job.Key, 0, "", "{}")

		if err := executor.executeRun(ctx, run.ID); err != nil {
			t.Fatalf("execute run: %v", err)
		}

		runRow, err := queries.RunGetByID(ctx, run.ID)
		if err != nil {
			t.Fatalf("load run: %v", err)
		}
		if runRow.Status != "failed" {
			t.Fatalf("expected run failed, got %q", runRow.Status)
		}
		if !strings.Contains(runRow.ErrorMessage, "panic in step panic_step") {
			t.Fatalf("expected panic message, got %q", runRow.ErrorMessage)
		}

		stepRows, err := queries.RunStepGetManyByRunID(ctx, run.ID)
		if err != nil {
			t.Fatalf("load run steps: %v", err)
		}
		panicRow := mustFindRunStep(t, stepRows, "panic_step")
		if panicRow.Status != "failed" {
			t.Fatalf("expected panic step failed, got %q", panicRow.Status)
		}
		if !strings.Contains(panicRow.ErrorMessage, "panic in step panic_step") {
			t.Fatalf("expected panic step error message, got %q", panicRow.ErrorMessage)
		}

		events := mustLoadRunEvents(t, ctx, queries, run.ID)
		assertRunEventSequence(t, events,
			[]string{"run_started", "step_started", "step_failed", "run_completed"},
			[]string{"", "panic_step", "panic_step", ""},
		)
		assertRunCompletedEventStatus(t, events, "failed")
	})

	t.Run("output_marshal_error_marks_step_failed", func(t *testing.T) {
		badJSONStep := Define[NoInput, exMarshalOutput]("emit_bad_json", func(_ context.Context, _ NoInput) (exMarshalOutput, error) {
			return exMarshalOutput{Bad: func() {}}, nil
		})
		job := NewJob("executor_marshal_failure").Add(badJSONStep).MustBuild()

		ctx, queries, executor := newExecutorTestHarness(t, job)
		run := createRunWithPendingSteps(t, ctx, queries, job.Key, 0, "", "{}")

		if err := executor.executeRun(ctx, run.ID); err != nil {
			t.Fatalf("execute run: %v", err)
		}

		runRow, err := queries.RunGetByID(ctx, run.ID)
		if err != nil {
			t.Fatalf("load run: %v", err)
		}
		if runRow.Status != "failed" {
			t.Fatalf("expected run failed, got %q", runRow.Status)
		}
		if !strings.Contains(runRow.ErrorMessage, "marshal output for step emit_bad_json") {
			t.Fatalf("expected marshal failure message, got %q", runRow.ErrorMessage)
		}

		stepRows, err := queries.RunStepGetManyByRunID(ctx, run.ID)
		if err != nil {
			t.Fatalf("load run steps: %v", err)
		}
		badStep := mustFindRunStep(t, stepRows, "emit_bad_json")
		if badStep.Status != "failed" {
			t.Fatalf("expected bad output step failed, got %q", badStep.Status)
		}
		if !strings.Contains(badStep.ErrorMessage, "json: unsupported type: func()") {
			t.Fatalf("expected marshal error details, got %q", badStep.ErrorMessage)
		}

		events := mustLoadRunEvents(t, ctx, queries, run.ID)
		assertRunEventSequence(t, events,
			[]string{"run_started", "step_started", "step_failed", "run_completed"},
			[]string{"", "emit_bad_json", "emit_bad_json", ""},
		)
		assertRunCompletedEventStatus(t, events, "failed")
	})

	t.Run("hydrate_input_decode_error_marks_run_failed", func(t *testing.T) {
		provider := Define[NoInput, exHydrateProviderOutput]("provider", func(_ context.Context, _ NoInput) (exHydrateProviderOutput, error) {
			return exHydrateProviderOutput{Value: 1}, nil
		})
		consumer := Define[exHydrateConsumerInput, exHydrateConsumerOutput]("consumer", func(_ context.Context, in exHydrateConsumerInput) (exHydrateConsumerOutput, error) {
			return exHydrateConsumerOutput{Value: in.Provider.Value + 1}, nil
		})
		job := NewJob("executor_hydrate_failure").Add(provider, consumer).MustBuild()

		ctx, queries, executor := newExecutorTestHarness(t, job)
		parentRun := createRunWithPendingSteps(t, ctx, queries, job.Key, 0, "", "{}")

		now := time.Now().UTC().Format(time.RFC3339Nano)
		_, err := queries.RunStepUpdateForComplete(ctx, db.RunStepUpdateForCompleteParams{
			Status:       "success",
			CompletedAt:  now,
			DurationMs:   1,
			OutputJson:   `{"value":`,
			ErrorMessage: "",
			LogExcerpt:   "seeded invalid json",
			RunID:        parentRun.ID,
			StepKey:      "provider",
		})
		if err != nil {
			t.Fatalf("seed parent step output: %v", err)
		}
		_, err = queries.RunUpdateForComplete(ctx, db.RunUpdateForCompleteParams{
			Status:       "success",
			CompletedAt:  now,
			ErrorMessage: "",
			ID:           parentRun.ID,
		})
		if err != nil {
			t.Fatalf("mark parent run complete: %v", err)
		}

		childRun := createRunWithPendingSteps(t, ctx, queries, job.Key, parentRun.ID, "consumer", "{}")
		if err := executor.executeRun(ctx, childRun.ID); err != nil {
			t.Fatalf("execute child run: %v", err)
		}

		runRow, err := queries.RunGetByID(ctx, childRun.ID)
		if err != nil {
			t.Fatalf("load child run: %v", err)
		}
		if runRow.Status != "failed" {
			t.Fatalf("expected child run failed, got %q", runRow.Status)
		}
		if !strings.Contains(runRow.ErrorMessage, "decode dependency provider into field Provider") {
			t.Fatalf("expected hydrate decode failure, got %q", runRow.ErrorMessage)
		}

		stepRows, err := queries.RunStepGetManyByRunID(ctx, childRun.ID)
		if err != nil {
			t.Fatalf("load child steps: %v", err)
		}
		providerRow := mustFindRunStep(t, stepRows, "provider")
		consumerRow := mustFindRunStep(t, stepRows, "consumer")
		if providerRow.Status != "skipped" {
			t.Fatalf("expected provider skipped due rerun target, got %q", providerRow.Status)
		}
		if consumerRow.Status != "failed" {
			t.Fatalf("expected consumer failed, got %q", consumerRow.Status)
		}
		if !strings.Contains(consumerRow.ErrorMessage, "decode dependency provider into field Provider") {
			t.Fatalf("expected hydrate failure on consumer step, got %q", consumerRow.ErrorMessage)
		}

		events := mustLoadRunEvents(t, ctx, queries, childRun.ID)
		assertRunEventSequence(t, events,
			[]string{"run_started", "step_skipped", "step_started", "step_failed", "run_completed"},
			[]string{"", "provider", "consumer", "consumer", ""},
		)
		assertRunCompletedEventStatus(t, events, "failed")
	})
}

func TestExecutorExecuteRun_RerunStepUsesParentOutputs(t *testing.T) {
	source := Define[NoInput, exSourceOutput]("source", func(_ context.Context, _ NoInput) (exSourceOutput, error) {
		return exSourceOutput{Value: 2}, nil
	})
	transform := Define[exTransformInput, exTransformOutput]("transform", func(_ context.Context, in exTransformInput) (exTransformOutput, error) {
		return exTransformOutput{Transformed: in.Source.Value + 3}, nil
	})
	publish := Define[exPublishInput, exPublishOutput]("publish", func(_ context.Context, _ exPublishInput) (exPublishOutput, error) {
		return exPublishOutput{Done: true}, nil
	})
	job := NewJob("executor_rerun").Add(source, transform, publish).MustBuild()

	ctx, queries, executor := newExecutorTestHarness(t, job)
	parentRun := createRunWithPendingSteps(t, ctx, queries, job.Key, 0, "", "{}")
	if err := executor.executeRun(ctx, parentRun.ID); err != nil {
		t.Fatalf("execute parent run: %v", err)
	}

	childRun := createRunWithPendingSteps(t, ctx, queries, job.Key, parentRun.ID, "transform", "{}")
	if err := executor.executeRun(ctx, childRun.ID); err != nil {
		t.Fatalf("execute child run: %v", err)
	}

	runRow, err := queries.RunGetByID(ctx, childRun.ID)
	if err != nil {
		t.Fatalf("load child run: %v", err)
	}
	if runRow.Status != "success" {
		t.Fatalf("expected child run success, got %q", runRow.Status)
	}
	if runRow.ErrorMessage != "" {
		t.Fatalf("expected empty child run error, got %q", runRow.ErrorMessage)
	}

	stepRows, err := queries.RunStepGetManyByRunID(ctx, childRun.ID)
	if err != nil {
		t.Fatalf("load child run steps: %v", err)
	}
	sourceRow := mustFindRunStep(t, stepRows, "source")
	transformRow := mustFindRunStep(t, stepRows, "transform")
	publishRow := mustFindRunStep(t, stepRows, "publish")

	if sourceRow.Status != "skipped" {
		t.Fatalf("expected source step skipped, got %q", sourceRow.Status)
	}
	if publishRow.Status != "skipped" {
		t.Fatalf("expected publish step skipped, got %q", publishRow.Status)
	}
	if transformRow.Status != "success" {
		t.Fatalf("expected transform step success, got %q", transformRow.Status)
	}

	var output exTransformOutput
	if err := json.Unmarshal([]byte(transformRow.OutputJson), &output); err != nil {
		t.Fatalf("decode transform output: %v", err)
	}
	if output.Transformed != 5 {
		t.Fatalf("expected transform output 5, got %d", output.Transformed)
	}

	events := mustLoadRunEvents(t, ctx, queries, childRun.ID)
	assertRunEventSequence(t, events,
		[]string{"run_started", "step_skipped", "step_started", "step_succeeded", "step_skipped", "run_completed"},
		[]string{"", "source", "transform", "transform", "publish", ""},
	)
	assertRunCompletedEventStatus(t, events, "success")

	skipOnePayload := mustEventPayload(t, events[1])
	if got := payloadString(skipOnePayload, "rerun_step_key"); got != "transform" {
		t.Fatalf("expected rerun_step_key transform on first skipped event, got %q", got)
	}
	skipTwoPayload := mustEventPayload(t, events[4])
	if got := payloadString(skipTwoPayload, "rerun_step_key"); got != "transform" {
		t.Fatalf("expected rerun_step_key transform on second skipped event, got %q", got)
	}
}

func TestExecutorExecuteRun_EventContract(t *testing.T) {
	t.Run("success_events_have_expected_fields", func(t *testing.T) {
		step := Define[NoInput, exSourceOutput]("op", func(_ context.Context, _ NoInput) (exSourceOutput, error) {
			return exSourceOutput{Value: 1}, nil
		})
		job := NewJob("event_contract_success").Add(step).MustBuild()

		ctx, queries, executor := newExecutorTestHarness(t, job)
		run := createRunWithPendingSteps(t, ctx, queries, job.Key, 0, "", "{}")
		if err := executor.executeRun(ctx, run.ID); err != nil {
			t.Fatalf("execute run: %v", err)
		}

		events := mustLoadRunEvents(t, ctx, queries, run.ID)
		assertRunEventSequence(t, events,
			[]string{"run_started", "step_started", "step_succeeded", "run_completed"},
			[]string{"", "op", "op", ""},
		)

		start := events[0]
		if start.Level != "info" {
			t.Fatalf("expected run_started level info, got %q", start.Level)
		}
		if start.Message != "run started" {
			t.Fatalf("expected run_started message, got %q", start.Message)
		}
		startPayload := mustEventPayload(t, start)
		if got := payloadInt64(startPayload, "run_id"); got != run.ID {
			t.Fatalf("expected run_started payload run_id %d, got %d", run.ID, got)
		}

		stepStart := events[1]
		if stepStart.Level != "info" {
			t.Fatalf("expected step_started level info, got %q", stepStart.Level)
		}
		if stepStart.Message != "step started" {
			t.Fatalf("expected step_started message, got %q", stepStart.Message)
		}
		if got := len(mustEventPayload(t, stepStart)); got != 0 {
			t.Fatalf("expected empty payload for step_started, got %d keys", got)
		}

		stepSuccess := events[2]
		if stepSuccess.Level != "info" {
			t.Fatalf("expected step_succeeded level info, got %q", stepSuccess.Level)
		}
		if stepSuccess.Message != "step succeeded" {
			t.Fatalf("expected step_succeeded message, got %q", stepSuccess.Message)
		}
		successPayload := mustEventPayload(t, stepSuccess)
		if duration := payloadFloat64(successPayload, "duration_ms"); duration < 0 {
			t.Fatalf("expected non-negative duration, got %f", duration)
		}

		completed := events[3]
		if completed.Level != "info" {
			t.Fatalf("expected run_completed level info, got %q", completed.Level)
		}
		if completed.Message != "run completed" {
			t.Fatalf("expected run_completed message, got %q", completed.Message)
		}
		completedPayload := mustEventPayload(t, completed)
		if got := payloadString(completedPayload, "status"); got != "success" {
			t.Fatalf("expected run_completed status success, got %q", got)
		}
	})

	t.Run("failure_events_have_expected_fields", func(t *testing.T) {
		step := Define[NoInput, exSourceOutput]("op", func(_ context.Context, _ NoInput) (exSourceOutput, error) {
			return exSourceOutput{}, fmt.Errorf("boom")
		})
		job := NewJob("event_contract_failure").Add(step).MustBuild()

		ctx, queries, executor := newExecutorTestHarness(t, job)
		run := createRunWithPendingSteps(t, ctx, queries, job.Key, 0, "", "{}")
		if err := executor.executeRun(ctx, run.ID); err != nil {
			t.Fatalf("execute run: %v", err)
		}

		events := mustLoadRunEvents(t, ctx, queries, run.ID)
		assertRunEventSequence(t, events,
			[]string{"run_started", "step_started", "step_failed", "run_completed"},
			[]string{"", "op", "op", ""},
		)

		stepFailed := events[2]
		if stepFailed.Level != "error" {
			t.Fatalf("expected step_failed level error, got %q", stepFailed.Level)
		}
		if !strings.Contains(stepFailed.Message, "boom") {
			t.Fatalf("expected step_failed message to contain boom, got %q", stepFailed.Message)
		}
		failedPayload := mustEventPayload(t, stepFailed)
		if duration := payloadFloat64(failedPayload, "duration_ms"); duration < 0 {
			t.Fatalf("expected non-negative duration, got %f", duration)
		}

		completedPayload := mustEventPayload(t, events[3])
		if got := payloadString(completedPayload, "status"); got != "failed" {
			t.Fatalf("expected run_completed status failed, got %q", got)
		}
	})
}

func TestExecutorExecuteRun_StepContextLoggerWritesRunEvents(t *testing.T) {
	step := Define[NoInput, exSourceOutput]("op", func(ctx context.Context, _ NoInput) (exSourceOutput, error) {
		LogInfo(ctx, "step emitted log", "component", "unit-test", "count", 1)
		return exSourceOutput{Value: 3}, nil
	})
	job := NewJob("event_step_logger").Add(step).MustBuild()

	ctx, queries, executor := newExecutorTestHarness(t, job)
	run := createRunWithPendingSteps(t, ctx, queries, job.Key, 0, "", "{}")
	if err := executor.executeRun(ctx, run.ID); err != nil {
		t.Fatalf("execute run: %v", err)
	}

	events := mustLoadRunEvents(t, ctx, queries, run.ID)
	assertRunEventSequence(t, events,
		[]string{"run_started", "step_started", "step_log", "step_succeeded", "run_completed"},
		[]string{"", "op", "op", "op", ""},
	)

	logEvent := events[2]
	if logEvent.Level != "info" {
		t.Fatalf("expected step_log level info, got %q", logEvent.Level)
	}
	if logEvent.Message != "step emitted log" {
		t.Fatalf("expected step_log message, got %q", logEvent.Message)
	}
	payload := mustEventPayload(t, logEvent)
	if got := payloadString(payload, "component"); got != "unit-test" {
		t.Fatalf("expected component=unit-test, got %q", got)
	}
	if got := payloadInt64(payload, "count"); got != 1 {
		t.Fatalf("expected count=1, got %d", got)
	}
	if got := payloadString(payload, "step_key"); got != "op" {
		t.Fatalf("expected step_key=op, got %q", got)
	}
}

func TestExecutorExecuteRun_AutomaticStdIOAndDefaultSlogCapture(t *testing.T) {
	step := Define[NoInput, exSourceOutput]("op", func(_ context.Context, _ NoInput) (exSourceOutput, error) {
		fmt.Println("stdout log line")
		fmt.Fprintln(os.Stderr, "stderr log line")
		slog.Info("default slog line", "component", "unit-test")
		return exSourceOutput{Value: 9}, nil
	})
	job := NewJob("event_stdio_logger").Add(step).MustBuild()

	ctx, queries, executor := newExecutorTestHarness(t, job)
	run := createRunWithPendingSteps(t, ctx, queries, job.Key, 0, "", "{}")
	if err := executor.executeRun(ctx, run.ID); err != nil {
		t.Fatalf("execute run: %v", err)
	}

	events := mustLoadRunEvents(t, ctx, queries, run.ID)

	foundStdout := false
	foundStderr := false
	foundSlog := false
	for _, event := range events {
		if event.EventType != "step_log" || event.StepKey != "op" {
			continue
		}
		switch event.Message {
		case "stdout log line":
			foundStdout = true
		case "stderr log line":
			foundStderr = true
		case "default slog line":
			foundSlog = true
		}
	}

	if !foundStdout {
		t.Fatalf("expected stdout log line to be captured as step_log event")
	}
	if !foundStderr {
		t.Fatalf("expected stderr log line to be captured as step_log event")
	}
	if !foundSlog {
		t.Fatalf("expected default slog line to be captured as step_log event")
	}
}

func TestExecutorExecuteRun_ConcurrentReadySteps(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	t.Cleanup(releaseAll)

	stepA := Define[NoInput, exParallelAOutput]("a", func(ctx context.Context, _ NoInput) (exParallelAOutput, error) {
		started <- "a"
		select {
		case <-ctx.Done():
			return exParallelAOutput{}, ctx.Err()
		case <-release:
		}
		return exParallelAOutput{Value: 1}, nil
	})
	stepB := Define[NoInput, exParallelBOutput]("b", func(ctx context.Context, _ NoInput) (exParallelBOutput, error) {
		started <- "b"
		select {
		case <-ctx.Done():
			return exParallelBOutput{}, ctx.Err()
		case <-release:
		}
		return exParallelBOutput{Value: 2}, nil
	})
	stepJoin := Define[exParallelJoinInput, exParallelJoinOutput]("join", func(_ context.Context, in exParallelJoinInput) (exParallelJoinOutput, error) {
		if in.A.Value != 1 || in.B.Value != 2 {
			return exParallelJoinOutput{}, fmt.Errorf("unexpected join inputs: %+v", in)
		}
		return exParallelJoinOutput{Done: true}, nil
	})
	job := NewJob("executor_parallel_ready_steps").Add(stepA, stepB, stepJoin).MustBuild()

	ctx, queries, executor := newExecutorTestHarness(t, job)
	executor.SetRunMaxConcurrentSteps(2)
	run := createRunWithPendingSteps(t, ctx, queries, job.Key, 0, "", "{}")

	done := make(chan error, 1)
	go func() {
		done <- executor.executeRun(ctx, run.ID)
	}()

	seen := map[string]bool{}
	timeout := time.NewTimer(2 * time.Second)
	defer timeout.Stop()
	for len(seen) < 2 {
		select {
		case key := <-started:
			seen[key] = true
		case <-timeout.C:
			releaseAll()
			t.Fatalf("expected both root steps to start before release, got starts=%v", seen)
		}
	}

	releaseAll()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("execute run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for concurrent run completion")
	}

	runRow, err := queries.RunGetByID(ctx, run.ID)
	if err != nil {
		t.Fatalf("load run: %v", err)
	}
	if runRow.Status != "success" {
		t.Fatalf("expected run success, got %q", runRow.Status)
	}

	steps, err := queries.RunStepGetManyByRunID(ctx, run.ID)
	if err != nil {
		t.Fatalf("load run steps: %v", err)
	}
	if got := mustFindRunStep(t, steps, "a").Status; got != "success" {
		t.Fatalf("expected step a success, got %q", got)
	}
	if got := mustFindRunStep(t, steps, "b").Status; got != "success" {
		t.Fatalf("expected step b success, got %q", got)
	}
	if got := mustFindRunStep(t, steps, "join").Status; got != "success" {
		t.Fatalf("expected step join success, got %q", got)
	}
}

func TestExecutorQueue_ConcurrentRuns(t *testing.T) {
	type concurrentRunOutput struct {
		Done bool `json:"done"`
	}

	started := make(chan int64, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	t.Cleanup(releaseAll)

	step := Define[NoInput, concurrentRunOutput]("op", func(ctx context.Context, _ NoInput) (concurrentRunOutput, error) {
		meta, ok := RunMetaFromContext(ctx)
		if !ok {
			return concurrentRunOutput{}, fmt.Errorf("missing run meta from context")
		}
		started <- meta.RunID
		select {
		case <-ctx.Done():
			return concurrentRunOutput{}, ctx.Err()
		case <-release:
		}
		return concurrentRunOutput{Done: true}, nil
	})
	job := NewJob("executor_parallel_runs").Add(step).MustBuild()

	ctx, queries, executor := newExecutorTestHarness(t, job)
	executor.SetRunMaxConcurrentRuns(2)
	executor.SetRunMaxConcurrentSteps(1)
	t.Cleanup(func() {
		close(executor.queue)
	})

	run1 := createRunWithPendingSteps(t, ctx, queries, job.Key, 0, "", "{}")
	run2 := createRunWithPendingSteps(t, ctx, queries, job.Key, 0, "", "{}")

	executor.EnqueueRun(run1.ID)
	executor.EnqueueRun(run2.ID)

	seen := map[int64]bool{}
	timeout := time.NewTimer(2 * time.Second)
	defer timeout.Stop()
	for len(seen) < 2 {
		select {
		case runID := <-started:
			seen[runID] = true
		case <-timeout.C:
			releaseAll()
			t.Fatalf("expected two concurrent run starts, got %v", seen)
		}
	}

	releaseAll()

	idleTimeout := time.NewTimer(5 * time.Second)
	defer idleTimeout.Stop()
	for !executor.IsIdle() {
		select {
		case <-idleTimeout.C:
			t.Fatalf("timed out waiting for queued runs to complete; active=%d queue=%d", executor.ActiveRuns(), executor.QueueDepth())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	run1Row, err := queries.RunGetByID(ctx, run1.ID)
	if err != nil {
		t.Fatalf("load run1: %v", err)
	}
	run2Row, err := queries.RunGetByID(ctx, run2.ID)
	if err != nil {
		t.Fatalf("load run2: %v", err)
	}
	if run1Row.Status != "success" {
		t.Fatalf("expected run1 success, got %q", run1Row.Status)
	}
	if run2Row.Status != "success" {
		t.Fatalf("expected run2 success, got %q", run2Row.Status)
	}
}

func TestExecutorQueue_SubprocessLaunchFailureMarksRunFailed(t *testing.T) {
	step := Define[NoInput, exSourceOutput]("op", func(_ context.Context, _ NoInput) (exSourceOutput, error) {
		return exSourceOutput{Value: 1}, nil
	})
	job := NewJob("executor_subprocess_launch_failure").Add(step).MustBuild()

	ctx, queries, executor := newExecutorTestHarness(t, job)
	executor.SetExecutionMode(ExecutionModeSubprocess)
	executor.SetWorkerBinary("/path/does/not/exist/daggo-worker")

	run := createRunWithPendingSteps(t, ctx, queries, job.Key, 0, "", "{}")
	executor.EnqueueRun(run.ID)

	runRow, err := queries.RunGetByID(ctx, run.ID)
	if err != nil {
		t.Fatalf("load run: %v", err)
	}
	if runRow.Status != "failed" {
		t.Fatalf("expected run status failed, got %q", runRow.Status)
	}
	if !strings.Contains(runRow.ErrorMessage, "failed to start worker process") {
		t.Fatalf("expected worker launch failure in error message, got %q", runRow.ErrorMessage)
	}

	events := mustLoadRunEvents(t, ctx, queries, run.ID)
	foundLaunchFailure := false
	foundRunFailed := false
	for _, event := range events {
		if event.EventType == "run_worker_launch_failed" {
			foundLaunchFailure = true
		}
		if event.EventType == "run_failed" {
			foundRunFailed = true
		}
	}
	if !foundLaunchFailure {
		t.Fatalf("expected run_worker_launch_failed event")
	}
	if !foundRunFailed {
		t.Fatalf("expected run_failed event")
	}
}

func TestExecutorMarkRunCanceled_UpdatesRunAndSteps(t *testing.T) {
	stepA := Define[NoInput, exSourceOutput]("a", func(_ context.Context, _ NoInput) (exSourceOutput, error) {
		return exSourceOutput{Value: 1}, nil
	})
	stepB := Define[exTransformInput, exTransformOutput]("b", func(_ context.Context, in exTransformInput) (exTransformOutput, error) {
		return exTransformOutput{Transformed: in.Source.Value + 1}, nil
	})
	job := NewJob("executor_mark_canceled").Add(stepA, stepB).MustBuild()

	ctx, queries, executor := newExecutorTestHarness(t, job)
	run := createRunWithPendingSteps(t, ctx, queries, job.Key, 0, "", "{}")

	if err := executor.markRunCanceled(ctx, run.ID, "test cancel"); err != nil {
		t.Fatalf("mark run canceled: %v", err)
	}

	runRow, err := queries.RunGetByID(ctx, run.ID)
	if err != nil {
		t.Fatalf("load run: %v", err)
	}
	if runRow.Status != "canceled" {
		t.Fatalf("expected canceled run status, got %q", runRow.Status)
	}
	if !strings.Contains(runRow.ErrorMessage, "test cancel") {
		t.Fatalf("expected cancel reason in error message, got %q", runRow.ErrorMessage)
	}

	steps, err := queries.RunStepGetManyByRunID(ctx, run.ID)
	if err != nil {
		t.Fatalf("load run steps: %v", err)
	}
	for _, step := range steps {
		if step.Status != "canceled" {
			t.Fatalf("expected step %s canceled, got %q", step.StepKey, step.Status)
		}
	}

	events := mustLoadRunEvents(t, ctx, queries, run.ID)
	foundRunCanceled := false
	stepCanceledCount := 0
	for _, event := range events {
		if event.EventType == "run_canceled" {
			foundRunCanceled = true
		}
		if event.EventType == "step_canceled" {
			stepCanceledCount++
		}
	}
	if !foundRunCanceled {
		t.Fatalf("expected run_canceled event")
	}
	if stepCanceledCount != len(steps) {
		t.Fatalf("expected %d step_canceled events, got %d", len(steps), stepCanceledCount)
	}
}

func TestExecutorTerminateRun_UnknownRunningProcessMarksCanceled(t *testing.T) {
	step := Define[NoInput, exSourceOutput]("op", func(_ context.Context, _ NoInput) (exSourceOutput, error) {
		return exSourceOutput{Value: 1}, nil
	})
	job := NewJob("executor_terminate_unknown_running").Add(step).MustBuild()

	ctx, queries, executor := newExecutorTestHarness(t, job)
	run := createRunWithPendingSteps(t, ctx, queries, job.Key, 0, "", "{}")

	if _, err := queries.RunUpdateForStart(ctx, db.RunUpdateForStartParams{
		Status:    "running",
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano),
		ID:        run.ID,
	}); err != nil {
		t.Fatalf("mark run running: %v", err)
	}

	if err := executor.TerminateRun(run.ID); err != nil {
		t.Fatalf("terminate run: %v", err)
	}

	runRow, err := queries.RunGetByID(ctx, run.ID)
	if err != nil {
		t.Fatalf("load run: %v", err)
	}
	if runRow.Status != "canceled" {
		t.Fatalf("expected canceled run status, got %q", runRow.Status)
	}
	if !strings.Contains(runRow.ErrorMessage, "terminated by operator") {
		t.Fatalf("expected termination reason, got %q", runRow.ErrorMessage)
	}
}

func TestExecutorExecuteRun_DoesNotRestartCanceledRun(t *testing.T) {
	step := Define[NoInput, exSourceOutput]("op", func(_ context.Context, _ NoInput) (exSourceOutput, error) {
		return exSourceOutput{Value: 1}, nil
	})
	job := NewJob("executor_no_restart_canceled").Add(step).MustBuild()

	ctx, queries, executor := newExecutorTestHarness(t, job)
	run := createRunWithPendingSteps(t, ctx, queries, job.Key, 0, "", "{}")

	if err := executor.markRunCanceled(ctx, run.ID, "test cancellation"); err != nil {
		t.Fatalf("mark canceled: %v", err)
	}
	if err := executor.executeRun(ctx, run.ID); err != nil {
		t.Fatalf("execute canceled run: %v", err)
	}

	runRow, err := queries.RunGetByID(ctx, run.ID)
	if err != nil {
		t.Fatalf("load run: %v", err)
	}
	if runRow.Status != "canceled" {
		t.Fatalf("expected canceled run status, got %q", runRow.Status)
	}

	events := mustLoadRunEvents(t, ctx, queries, run.ID)
	for _, event := range events {
		if event.EventType == "run_started" {
			t.Fatalf("did not expect run_started event for canceled run")
		}
	}
}

func newExecutorTestHarness(t *testing.T, job JobDefinition) (context.Context, *db.Queries, *Executor) {
	t.Helper()

	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	registry := NewRegistry()
	if err := registry.Register(job); err != nil {
		t.Fatalf("register job: %v", err)
	}
	if err := registry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("sync registry to db: %v", err)
	}

	executor := NewExecutor(queries, pool, registry, 16)
	return ctx, queries, executor
}

func createRunWithPendingSteps(
	t *testing.T,
	ctx context.Context,
	queries *db.Queries,
	jobKey string,
	parentRunID int64,
	rerunStepKey string,
	paramsJSON string,
) db.Run {
	t.Helper()

	jobRow, err := queries.JobGetByKey(ctx, jobKey)
	if err != nil {
		t.Fatalf("load job %q: %v", jobKey, err)
	}
	nodes, err := queries.JobNodeGetManyByJobID(ctx, jobRow.ID)
	if err != nil {
		t.Fatalf("load job nodes for %q: %v", jobKey, err)
	}
	if len(nodes) == 0 {
		t.Fatalf("expected job %q to have nodes", jobKey)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if strings.TrimSpace(paramsJSON) == "" {
		paramsJSON = "{}"
	}
	runKey := fmt.Sprintf("test_run_%d", time.Now().UTC().UnixNano())
	run, err := queries.RunCreate(ctx, db.RunCreateParams{
		RunKey:       runKey,
		JobID:        jobRow.ID,
		Status:       "queued",
		TriggeredBy:  "unit-test",
		ParamsJson:   paramsJSON,
		QueuedAt:     now,
		StartedAt:    "",
		CompletedAt:  "",
		ParentRunID:  parentRunID,
		RerunStepKey: rerunStepKey,
		ErrorMessage: "",
	})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	for _, node := range nodes {
		_, err := queries.RunStepCreate(ctx, db.RunStepCreateParams{
			RunID:        run.ID,
			JobNodeID:    node.ID,
			StepKey:      node.StepKey,
			Status:       "pending",
			Attempt:      1,
			StartedAt:    "",
			CompletedAt:  "",
			DurationMs:   0,
			OutputJson:   "{}",
			ErrorMessage: "",
			LogExcerpt:   "",
		})
		if err != nil {
			t.Fatalf("create run step %q: %v", node.StepKey, err)
		}
	}
	return run
}

func mustFindRunStep(t *testing.T, rows []db.RunStep, stepKey string) db.RunStep {
	t.Helper()
	for _, row := range rows {
		if row.StepKey == stepKey {
			return row
		}
	}
	t.Fatalf("run step %q not found", stepKey)
	return db.RunStep{}
}

func mustLoadRunEvents(t *testing.T, ctx context.Context, queries *db.Queries, runID int64) []db.RunEvent {
	t.Helper()
	rows, err := queries.RunEventGetManyByRunID(ctx, db.RunEventGetManyByRunIDParams{
		RunID:  runID,
		Limit:  200,
		Offset: 0,
	})
	if err != nil {
		t.Fatalf("load run events: %v", err)
	}
	return rows
}

func assertRunEventSequence(t *testing.T, events []db.RunEvent, wantTypes []string, wantStepKeys []string) {
	t.Helper()
	if len(events) != len(wantTypes) {
		t.Fatalf("expected %d events, got %d", len(wantTypes), len(events))
	}
	if len(wantStepKeys) != len(wantTypes) {
		t.Fatalf("invalid test: wantStepKeys len %d does not match wantTypes len %d", len(wantStepKeys), len(wantTypes))
	}

	for idx, row := range events {
		if row.EventType != wantTypes[idx] {
			t.Fatalf("event[%d] expected type %q, got %q", idx, wantTypes[idx], row.EventType)
		}
		if row.StepKey != wantStepKeys[idx] {
			t.Fatalf("event[%d] expected step key %q, got %q", idx, wantStepKeys[idx], row.StepKey)
		}
	}
}

func assertRunCompletedEventStatus(t *testing.T, events []db.RunEvent, wantStatus string) {
	t.Helper()
	var completed *db.RunEvent
	for idx := range events {
		if events[idx].EventType == "run_completed" {
			completed = &events[idx]
		}
	}
	if completed == nil {
		t.Fatalf("run_completed event not found")
	}

	payload := map[string]any{}
	if err := json.Unmarshal([]byte(completed.EventDataJson), &payload); err != nil {
		t.Fatalf("decode run_completed payload: %v", err)
	}
	gotStatus, _ := payload["status"].(string)
	if gotStatus != wantStatus {
		t.Fatalf("expected run_completed status %q, got %q", wantStatus, gotStatus)
	}
}

func mustEventPayload(t *testing.T, event db.RunEvent) map[string]any {
	t.Helper()
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(event.EventDataJson), &payload); err != nil {
		t.Fatalf("decode payload for event %s: %v", event.EventType, err)
	}
	return payload
}

func payloadString(payload map[string]any, key string) string {
	value, _ := payload[key].(string)
	return value
}

func payloadFloat64(payload map[string]any, key string) float64 {
	value, _ := payload[key].(float64)
	return value
}

func payloadInt64(payload map[string]any, key string) int64 {
	value, _ := payload[key].(float64)
	return int64(value)
}
