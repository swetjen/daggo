package dag

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/swetjen/daggo/db"
)

type Executor struct {
	queries  db.Store
	pool     *sql.DB
	registry *Registry

	executionMode string
	workerBinary  string
	workerCommand []string

	processesMu sync.Mutex
	processes   map[int64]runProcess
	terminated  map[int64]struct{}

	queue                 chan int64
	once                  sync.Once
	activeRuns            atomic.Int64
	runMaxConcurrentRuns  atomic.Int64
	runMaxConcurrentSteps atomic.Int64
}

type runProcess struct {
	pid       int
	startedAt time.Time
}

const (
	ExecutionModeInProcess  = "in_process"
	ExecutionModeSubprocess = "subprocess"

	backfillStatusRunning         = "running"
	backfillStatusCompleted       = "completed"
	backfillStatusFailed          = "failed"
	backfillPartitionStatusDone   = "materialized"
	backfillPartitionStatusFailed = "failed_downstream"
)

func NewExecutor(queries db.Store, pool *sql.DB, registry *Registry, queueSize int) *Executor {
	if queueSize <= 0 {
		queueSize = 128
	}
	executor := &Executor{
		queries:    queries,
		pool:       pool,
		registry:   registry,
		queue:      make(chan int64, queueSize),
		processes:  make(map[int64]runProcess),
		terminated: make(map[int64]struct{}),
	}
	executor.SetExecutionMode(ExecutionModeInProcess)
	executor.SetWorkerCommand("daggo-worker")
	executor.runMaxConcurrentRuns.Store(1)
	executor.runMaxConcurrentSteps.Store(1)
	return executor
}

func normalizeExecutionMode(mode string) string {
	switch strings.TrimSpace(strings.ToLower(mode)) {
	case "", ExecutionModeInProcess:
		return ExecutionModeInProcess
	case ExecutionModeSubprocess:
		return ExecutionModeSubprocess
	default:
		return ExecutionModeInProcess
	}
}

func (e *Executor) SetExecutionMode(mode string) {
	if e == nil {
		return
	}
	e.executionMode = normalizeExecutionMode(mode)
}

func (e *Executor) ExecutionMode() string {
	if e == nil {
		return ExecutionModeInProcess
	}
	return normalizeExecutionMode(e.executionMode)
}

func (e *Executor) SetWorkerBinary(path string) {
	if e == nil {
		return
	}
	e.workerBinary = strings.TrimSpace(path)
}

func (e *Executor) SetWorkerCommand(args ...string) {
	if e == nil {
		return
	}
	if len(args) == 0 {
		e.workerCommand = []string{"daggo-worker"}
		return
	}
	command := make([]string, 0, len(args))
	for _, arg := range args {
		if trimmed := strings.TrimSpace(arg); trimmed != "" {
			command = append(command, trimmed)
		}
	}
	if len(command) == 0 {
		command = []string{"daggo-worker"}
	}
	e.workerCommand = command
}

func (e *Executor) workerCommandArgs() []string {
	if e == nil || len(e.workerCommand) == 0 {
		return []string{"daggo-worker"}
	}
	out := make([]string, len(e.workerCommand))
	copy(out, e.workerCommand)
	return out
}

func (e *Executor) workerBinaryPath() string {
	if e == nil {
		return ""
	}
	if strings.TrimSpace(e.workerBinary) != "" {
		return strings.TrimSpace(e.workerBinary)
	}
	executablePath, err := os.Executable()
	if err != nil {
		return ""
	}
	return executablePath
}

func (e *Executor) SetRunMaxConcurrentRuns(maxRuns int) {
	if e == nil {
		return
	}
	if maxRuns <= 0 {
		maxRuns = 1
	}
	e.runMaxConcurrentRuns.Store(int64(maxRuns))
}

func (e *Executor) RunMaxConcurrentRuns() int {
	if e == nil {
		return 1
	}
	configured := int(e.runMaxConcurrentRuns.Load())
	if configured <= 0 {
		return 1
	}
	return configured
}

func (e *Executor) SetRunMaxConcurrentSteps(maxSteps int) {
	if e == nil {
		return
	}
	if maxSteps <= 0 {
		maxSteps = 1
	}
	e.runMaxConcurrentSteps.Store(int64(maxSteps))
}

func (e *Executor) RunMaxConcurrentSteps() int {
	if e == nil {
		return 1
	}
	configured := int(e.runMaxConcurrentSteps.Load())
	if configured <= 0 {
		return 1
	}
	return configured
}

func (e *Executor) EnqueueRun(runID int64) {
	if e == nil || runID <= 0 {
		return
	}
	if e.ExecutionMode() == ExecutionModeSubprocess {
		e.launchRunSubprocess(context.Background(), runID)
		return
	}
	e.once.Do(func() {
		workers := e.RunMaxConcurrentRuns()
		for worker := 0; worker < workers; worker++ {
			go e.loop()
		}
	})
	e.queue <- runID
}

func (e *Executor) launchRunSubprocess(ctx context.Context, runID int64) {
	binaryPath := e.workerBinaryPath()
	if strings.TrimSpace(binaryPath) == "" {
		message := "worker binary path is empty"
		slog.Error("daggo: failed to start run worker", "run_id", runID, "err", message)
		_ = e.failRun(ctx, runID, message)
		return
	}

	args := append(e.workerCommandArgs(), "--run-id", strconv.FormatInt(runID, 10))
	cmd := exec.Command(binaryPath, args...)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	cmd.Stdin = nil
	cmd.Env = os.Environ()

	if err := cmd.Start(); err != nil {
		message := fmt.Sprintf("failed to start worker process: %v", err)
		slog.Error("daggo: failed to start run worker", "run_id", runID, "binary", binaryPath, "err", err)
		_ = e.addEvent(ctx, runID, "", "run_worker_launch_failed", "error", "worker launch failed", map[string]any{
			"binary": binaryPath,
			"args":   args,
			"error":  err.Error(),
		})
		_ = e.failRun(ctx, runID, message)
		return
	}

	pid := cmd.Process.Pid
	now := time.Now().UTC()
	hostname, _ := os.Hostname()

	e.processesMu.Lock()
	e.processes[runID] = runProcess{pid: pid, startedAt: now}
	e.processesMu.Unlock()

	slog.Info("daggo: launched run worker", "run_id", runID, "pid", pid, "binary", binaryPath)
	_ = e.addEvent(ctx, runID, "", "run_worker_started", "info", "worker process started", map[string]any{
		"pid":        pid,
		"hostname":   hostname,
		"binary":     binaryPath,
		"args":       args,
		"started_at": now.Format(time.RFC3339Nano),
	})

	go e.waitForRunSubprocess(runID, pid, cmd)
}

func (e *Executor) waitForRunSubprocess(runID int64, pid int, cmd *exec.Cmd) {
	waitErr := cmd.Wait()

	e.processesMu.Lock()
	delete(e.processes, runID)
	_, wasTerminated := e.terminated[runID]
	if wasTerminated {
		delete(e.terminated, runID)
	}
	e.processesMu.Unlock()

	exitCode := 0
	if cmd.ProcessState != nil {
		exitCode = cmd.ProcessState.ExitCode()
	}
	level := "info"
	message := "worker process exited"
	if waitErr != nil {
		level = "warn"
		message = "worker process exited with error"
	}

	_ = e.addEvent(context.Background(), runID, "", "run_worker_exited", level, message, map[string]any{
		"pid":       pid,
		"exit_code": exitCode,
		"error":     errorString(waitErr),
	})

	if wasTerminated {
		_ = e.markRunCanceled(context.Background(), runID, "terminated by operator")
		return
	}

	run, err := e.queries.RunGetByID(context.Background(), runID)
	if err != nil {
		return
	}
	if run.Status == "queued" || run.Status == "running" {
		errMessage := "worker process exited before run reached terminal state"
		if waitErr != nil {
			errMessage = fmt.Sprintf("%s: %v", errMessage, waitErr)
		}
		_ = e.failRun(context.Background(), runID, errMessage)
	}
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (e *Executor) TerminateRun(runID int64) error {
	if e == nil || runID <= 0 {
		return fmt.Errorf("run_id is required")
	}
	e.processesMu.Lock()
	process, ok := e.processes[runID]
	if ok {
		e.terminated[runID] = struct{}{}
	}
	e.processesMu.Unlock()
	if !ok {
		run, err := e.queries.RunGetByID(context.Background(), runID)
		if err == nil {
			status := normalizeExecutionStatus(run.Status)
			if !isTerminalExecutionStatus(status) {
				return e.markRunCanceled(context.Background(), runID, "terminated by operator")
			}
		}
		return fmt.Errorf("run %d is not running in this process", runID)
	}

	proc, err := os.FindProcess(process.pid)
	if err != nil {
		e.clearTerminated(runID)
		return err
	}
	if err := proc.Kill(); err != nil {
		e.clearTerminated(runID)
		return err
	}
	_ = e.markRunCanceled(context.Background(), runID, "terminated by operator")
	_ = e.addEvent(context.Background(), runID, "", "run_worker_terminated", "warn", "worker process terminated", map[string]any{
		"pid": process.pid,
	})
	return nil
}

func (e *Executor) clearTerminated(runID int64) {
	if e == nil || runID <= 0 {
		return
	}
	e.processesMu.Lock()
	delete(e.terminated, runID)
	e.processesMu.Unlock()
}

func (e *Executor) markRunCanceled(ctx context.Context, runID int64, reason string) error {
	if e == nil || runID <= 0 {
		return fmt.Errorf("run_id is required")
	}

	run, err := e.queries.RunGetByID(ctx, runID)
	if err != nil {
		return err
	}
	currentStatus := normalizeExecutionStatus(run.Status)
	if currentStatus == "success" || currentStatus == "failed" || currentStatus == "canceled" {
		return nil
	}

	now := time.Now().UTC()
	completedAt := now.Format(time.RFC3339Nano)
	message := strings.TrimSpace(reason)
	if message == "" {
		message = "terminated by operator"
	}
	if _, err := e.queries.RunUpdateForComplete(ctx, db.RunUpdateForCompleteParams{
		Status:       "canceled",
		CompletedAt:  completedAt,
		ErrorMessage: message,
		ID:           runID,
	}); err != nil {
		return err
	}

	steps, err := e.queries.RunStepGetManyByRunID(ctx, runID)
	if err == nil {
		for _, step := range steps {
			status := normalizeExecutionStatus(step.Status)
			if status == "success" || status == "failed" || status == "skipped" || status == "canceled" {
				continue
			}
			durationMs := stepDurationAt(step, now)
			outputJSON := strings.TrimSpace(step.OutputJson)
			if outputJSON == "" {
				outputJSON = "{}"
			}
			_, _ = e.queries.RunStepUpdateForComplete(ctx, db.RunStepUpdateForCompleteParams{
				Status:       "canceled",
				CompletedAt:  completedAt,
				DurationMs:   durationMs,
				OutputJson:   outputJSON,
				ErrorMessage: message,
				LogExcerpt:   "step canceled",
				RunID:        runID,
				StepKey:      step.StepKey,
			})
			_ = e.addEvent(ctx, runID, step.StepKey, "step_canceled", "warn", "step canceled", map[string]any{
				"reason": message,
			})
		}
	}

	_ = e.addEvent(ctx, runID, "", "run_canceled", "warn", "run canceled", map[string]any{
		"reason": message,
	})
	if reconcileErr := e.reconcileBackfillForRun(ctx, runID, "canceled", message); reconcileErr != nil {
		slog.Error("daggo: failed to reconcile backfill state after run cancellation", "run_id", runID, "err", reconcileErr)
	}
	return nil
}

func normalizeExecutionStatus(status string) string {
	normalized := strings.TrimSpace(strings.ToLower(status))
	if normalized == "cancelled" {
		return "canceled"
	}
	return normalized
}

func isTerminalExecutionStatus(status string) bool {
	switch normalizeExecutionStatus(status) {
	case "success", "failed", "canceled":
		return true
	default:
		return false
	}
}

func stepDurationAt(step db.RunStep, now time.Time) int64 {
	if step.DurationMs > 0 {
		return step.DurationMs
	}
	startedAt := strings.TrimSpace(step.StartedAt)
	if startedAt == "" {
		return 0
	}
	started, err := time.Parse(time.RFC3339Nano, startedAt)
	if err != nil {
		return 0
	}
	duration := now.Sub(started).Milliseconds()
	if duration < 0 {
		return 0
	}
	return duration
}

func (e *Executor) loop() {
	for runID := range e.queue {
		e.activeRuns.Add(1)
		func() {
			defer e.activeRuns.Add(-1)
			if err := e.executeRun(context.Background(), runID); err != nil {
				slog.Error("daggo: run execution failed", "run_id", runID, "err", err)
			}
		}()
	}
}

func (e *Executor) ExecuteRun(ctx context.Context, runID int64) error {
	if e == nil || runID <= 0 {
		return fmt.Errorf("run_id is required")
	}
	return e.executeRun(ctx, runID)
}

func (e *Executor) IsIdle() bool {
	if e == nil {
		return true
	}
	if e.ExecutionMode() == ExecutionModeSubprocess {
		return e.ActiveRuns() == 0
	}
	return e.activeRuns.Load() == 0 && len(e.queue) == 0
}

func (e *Executor) ActiveRuns() int64 {
	if e == nil {
		return 0
	}
	if e.ExecutionMode() == ExecutionModeSubprocess {
		e.processesMu.Lock()
		defer e.processesMu.Unlock()
		return int64(len(e.processes))
	}
	return e.activeRuns.Load()
}

func (e *Executor) QueueDepth() int {
	if e == nil {
		return 0
	}
	if e.ExecutionMode() == ExecutionModeSubprocess {
		return 0
	}
	return len(e.queue)
}

func (e *Executor) executeRun(ctx context.Context, runID int64) error {
	run, err := e.queries.RunGetByID(ctx, runID)
	if err != nil {
		return err
	}
	if isTerminalExecutionStatus(run.Status) {
		return nil
	}

	latestRun, err := e.queries.RunGetByID(ctx, run.ID)
	if err == nil {
		run = latestRun
	}
	if isTerminalExecutionStatus(run.Status) {
		return nil
	}

	jobRow, err := e.queries.JobGetByID(ctx, run.JobID)
	if err != nil {
		_ = e.failRun(ctx, run.ID, fmt.Sprintf("failed to load job: %v", err))
		return err
	}
	job, ok := e.registry.JobByKey(jobRow.JobKey)
	if !ok {
		err := fmt.Errorf("job %q is not registered in memory", jobRow.JobKey)
		_ = e.failRun(ctx, run.ID, err.Error())
		return err
	}

	steps, err := e.queries.RunStepGetManyByRunID(ctx, run.ID)
	if err != nil {
		_ = e.failRun(ctx, run.ID, fmt.Sprintf("failed to load run steps: %v", err))
		return err
	}
	stepRows := make(map[string]db.RunStep, len(steps))
	for _, row := range steps {
		stepRows[row.StepKey] = row
	}

	order, err := topologicalOrder(job)
	if err != nil {
		_ = e.failRun(ctx, run.ID, fmt.Sprintf("invalid job graph: %v", err))
		return err
	}

	params := map[string]any{}
	if strings.TrimSpace(run.ParamsJson) != "" {
		_ = json.Unmarshal([]byte(run.ParamsJson), &params)
	}
	partitionMeta, partitionMetaErr := e.loadRunPartitionMeta(ctx, run.ID)
	if partitionMetaErr != nil {
		slog.Warn("daggo: failed to load run partition target", "run_id", run.ID, "err", partitionMetaErr)
	}

	outputs := map[string]json.RawMessage{}
	if run.ParentRunID > 0 {
		parentSteps, err := e.queries.RunStepGetManyByRunID(ctx, run.ParentRunID)
		if err == nil {
			for _, parentStep := range parentSteps {
				if parentStep.Status == "success" {
					outputs[parentStep.StepKey] = json.RawMessage(parentStep.OutputJson)
				}
			}
		}
	}

	if _, err := e.queries.RunUpdateForStart(ctx, db.RunUpdateForStartParams{
		Status:    "running",
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano),
		ID:        run.ID,
	}); err != nil {
		return err
	}
	_ = e.addEvent(ctx, run.ID, "", "run_started", "info", "run started", map[string]any{"run_id": run.ID})

	maxConcurrentSteps := e.RunMaxConcurrentSteps()
	captureGlobalLogs := e.RunMaxConcurrentRuns() <= 1 && maxConcurrentSteps <= 1
	hadFailure, failureMessage := e.executeRunSteps(ctx, run, job, order, stepRows, outputs, params, partitionMeta, maxConcurrentSteps, captureGlobalLogs)

	status := "success"
	if hadFailure {
		status = "failed"
	}

	freshRun, err := e.queries.RunGetByID(ctx, run.ID)
	if err == nil && normalizeExecutionStatus(freshRun.Status) == "canceled" {
		return nil
	}

	if _, err := e.queries.RunUpdateForComplete(ctx, db.RunUpdateForCompleteParams{
		Status:       status,
		CompletedAt:  time.Now().UTC().Format(time.RFC3339Nano),
		ErrorMessage: failureMessage,
		ID:           run.ID,
	}); err != nil {
		return err
	}
	_ = e.addEvent(ctx, run.ID, "", "run_completed", "info", "run completed", map[string]any{"status": status})
	if err := e.reconcileBackfillForRun(ctx, run.ID, status, failureMessage); err != nil {
		slog.Error("daggo: failed to reconcile backfill state after run completion", "run_id", run.ID, "err", err)
	}
	return nil
}

type stepExecutionResult struct {
	stepKey      string
	status       string
	output       json.RawMessage
	errorMessage string
}

func (e *Executor) executeRunSteps(
	ctx context.Context,
	run db.Run,
	job JobDefinition,
	order []StepDefinition,
	stepRows map[string]db.RunStep,
	outputs map[string]json.RawMessage,
	params map[string]any,
	partitionMeta *RunPartitionMeta,
	maxConcurrentSteps int,
	captureGlobalLogs bool,
) (bool, string) {
	for _, stepDef := range order {
		if _, ok := stepRows[stepDef.Key]; !ok {
			return true, "run step rows are incomplete"
		}
	}

	if run.RerunStepKey != "" || maxConcurrentSteps <= 1 || len(order) <= 1 {
		return e.executeRunStepsSerial(ctx, run, job, order, stepRows, outputs, params, partitionMeta, captureGlobalLogs)
	}
	return e.executeRunStepsConcurrent(ctx, run, job, order, stepRows, outputs, params, partitionMeta, maxConcurrentSteps)
}

func (e *Executor) executeRunStepsSerial(
	ctx context.Context,
	run db.Run,
	job JobDefinition,
	order []StepDefinition,
	stepRows map[string]db.RunStep,
	outputs map[string]json.RawMessage,
	params map[string]any,
	partitionMeta *RunPartitionMeta,
	captureGlobalLogs bool,
) (bool, string) {
	hadFailure := false
	failureMessage := ""

	for _, stepDef := range order {
		stepRow := stepRows[stepDef.Key]
		if run.RerunStepKey != "" && run.RerunStepKey != stepDef.Key {
			_, _ = e.queries.RunStepUpdateForComplete(ctx, db.RunStepUpdateForCompleteParams{
				Status:       "skipped",
				CompletedAt:  time.Now().UTC().Format(time.RFC3339Nano),
				DurationMs:   0,
				OutputJson:   stepRow.OutputJson,
				ErrorMessage: "",
				LogExcerpt:   "rerun skipped",
				RunID:        run.ID,
				StepKey:      stepDef.Key,
			})
			stepRow.Status = "skipped"
			stepRows[stepDef.Key] = stepRow
			_ = e.addEvent(ctx, run.ID, stepDef.Key, "step_skipped", "info", "step skipped due to rerun target", map[string]any{"rerun_step_key": run.RerunStepKey})
			continue
		}

		dependencyUnavailable := false
		for _, dep := range stepDef.DependsOn {
			if _, exists := outputs[dep]; !exists {
				dependencyUnavailable = true
				break
			}
		}
		if dependencyUnavailable {
			_, _ = e.queries.RunStepUpdateForComplete(ctx, db.RunStepUpdateForCompleteParams{
				Status:       "skipped",
				CompletedAt:  time.Now().UTC().Format(time.RFC3339Nano),
				DurationMs:   0,
				OutputJson:   "{}",
				ErrorMessage: "dependency output unavailable",
				LogExcerpt:   "dependency output unavailable",
				RunID:        run.ID,
				StepKey:      stepDef.Key,
			})
			stepRow.Status = "skipped"
			stepRows[stepDef.Key] = stepRow
			_ = e.addEvent(ctx, run.ID, stepDef.Key, "step_skipped", "warn", "step skipped due to failed dependency", map[string]any{})
			continue
		}

		result := e.executeSingleStep(ctx, run.ID, job.Key, stepDef, stepRow.Attempt, params, partitionMeta, outputs, captureGlobalLogs)
		stepRow.Status = result.status
		stepRow.ErrorMessage = result.errorMessage
		if result.status == "success" {
			stepRow.OutputJson = string(result.output)
			outputs[stepDef.Key] = result.output
		}
		stepRows[stepDef.Key] = stepRow

		if result.status == "failed" {
			hadFailure = true
			if failureMessage == "" {
				failureMessage = result.errorMessage
			}
		}
	}

	return hadFailure, failureMessage
}

func (e *Executor) executeRunStepsConcurrent(
	ctx context.Context,
	run db.Run,
	job JobDefinition,
	order []StepDefinition,
	stepRows map[string]db.RunStep,
	outputs map[string]json.RawMessage,
	params map[string]any,
	partitionMeta *RunPartitionMeta,
	maxConcurrentSteps int,
) (bool, string) {
	stepByKey := make(map[string]StepDefinition, len(order))
	dependentsByKey := make(map[string][]string, len(order))
	remainingDepsByKey := make(map[string]int, len(order))
	statusByKey := make(map[string]string, len(order))
	orderByKey := make(map[string]int, len(order))

	for idx, stepDef := range order {
		stepByKey[stepDef.Key] = stepDef
		remainingDepsByKey[stepDef.Key] = len(stepDef.DependsOn)
		statusByKey[stepDef.Key] = "pending"
		orderByKey[stepDef.Key] = idx
		for _, dep := range stepDef.DependsOn {
			dependentsByKey[dep] = append(dependentsByKey[dep], stepDef.Key)
		}
	}
	for parentKey, dependents := range dependentsByKey {
		sort.Slice(dependents, func(i, j int) bool {
			return orderByKey[dependents[i]] < orderByKey[dependents[j]]
		})
		dependentsByKey[parentKey] = dependents
	}

	ready := make([]string, 0, len(order))
	for _, stepDef := range order {
		if remainingDepsByKey[stepDef.Key] == 0 {
			ready = append(ready, stepDef.Key)
		}
	}
	sort.Slice(ready, func(i, j int) bool {
		return orderByKey[ready[i]] < orderByKey[ready[j]]
	})

	workerCount := maxConcurrentSteps
	if workerCount <= 0 {
		workerCount = 1
	}
	if workerCount > len(order) {
		workerCount = len(order)
	}

	workQueue := make(chan string)
	results := make(chan stepExecutionResult)
	var workers sync.WaitGroup
	var outputsMu sync.RWMutex
	attemptByKey := make(map[string]int64, len(stepRows))
	for stepKey, row := range stepRows {
		attemptByKey[stepKey] = row.Attempt
	}

	for worker := 0; worker < workerCount; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for stepKey := range workQueue {
				stepDef := stepByKey[stepKey]
				snapshot := cloneOutputs(outputs, &outputsMu)
				results <- e.executeSingleStep(ctx, run.ID, job.Key, stepDef, attemptByKey[stepKey], params, partitionMeta, snapshot, false)
			}
		}()
	}

	markReady := func(stepKey string) {
		ready = append(ready, stepKey)
		sort.Slice(ready, func(i, j int) bool {
			return orderByKey[ready[i]] < orderByKey[ready[j]]
		})
	}

	hadFailure := false
	failureMessage := ""
	completed := 0
	running := 0
	total := len(order)

	processCompleted := func(parentKey string) {
		queue := []string{parentKey}
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]
			for _, dependentKey := range dependentsByKey[current] {
				remainingDepsByKey[dependentKey]--
				if remainingDepsByKey[dependentKey] != 0 {
					continue
				}

				allDepsSucceeded := true
				for _, depKey := range stepByKey[dependentKey].DependsOn {
					if statusByKey[depKey] != "success" {
						allDepsSucceeded = false
						break
					}
				}
				if allDepsSucceeded {
					markReady(dependentKey)
					continue
				}

				_, _ = e.queries.RunStepUpdateForComplete(ctx, db.RunStepUpdateForCompleteParams{
					Status:       "skipped",
					CompletedAt:  time.Now().UTC().Format(time.RFC3339Nano),
					DurationMs:   0,
					OutputJson:   "{}",
					ErrorMessage: "dependency output unavailable",
					LogExcerpt:   "dependency output unavailable",
					RunID:        run.ID,
					StepKey:      dependentKey,
				})
				stepRow := stepRows[dependentKey]
				stepRow.Status = "skipped"
				stepRow.ErrorMessage = "dependency output unavailable"
				stepRows[dependentKey] = stepRow
				statusByKey[dependentKey] = "skipped"
				completed++
				_ = e.addEvent(ctx, run.ID, dependentKey, "step_skipped", "warn", "step skipped due to failed dependency", map[string]any{})
				queue = append(queue, dependentKey)
			}
		}
	}

	for completed < total {
		for running < workerCount && len(ready) > 0 {
			stepKey := ready[0]
			ready = ready[1:]
			if statusByKey[stepKey] != "pending" {
				continue
			}
			statusByKey[stepKey] = "running"
			running++
			workQueue <- stepKey
		}

		if running == 0 {
			hadFailure = true
			if failureMessage == "" {
				failureMessage = "run step scheduling stalled before completion"
			}
			break
		}

		result := <-results
		running--
		completed++
		statusByKey[result.stepKey] = result.status

		stepRow := stepRows[result.stepKey]
		stepRow.Status = result.status
		stepRow.ErrorMessage = result.errorMessage
		if result.status == "success" {
			stepRow.OutputJson = string(result.output)
			outputsMu.Lock()
			outputs[result.stepKey] = result.output
			outputsMu.Unlock()
		} else if result.status == "failed" {
			hadFailure = true
			if failureMessage == "" {
				failureMessage = result.errorMessage
			}
		}
		stepRows[result.stepKey] = stepRow
		processCompleted(result.stepKey)
	}

	close(workQueue)
	workers.Wait()
	return hadFailure, failureMessage
}

func (e *Executor) executeSingleStep(
	ctx context.Context,
	runID int64,
	jobKey string,
	stepDef StepDefinition,
	stepAttempt int64,
	params map[string]any,
	partitionMeta *RunPartitionMeta,
	outputs map[string]json.RawMessage,
	captureGlobalLogs bool,
) stepExecutionResult {
	result := stepExecutionResult{
		stepKey: stepDef.Key,
		status:  "failed",
		output:  nil,
	}

	stepStart := time.Now()
	if _, err := e.queries.RunStepUpdateForStart(ctx, db.RunStepUpdateForStartParams{
		Status:    "running",
		StartedAt: stepStart.UTC().Format(time.RFC3339Nano),
		RunID:     runID,
		StepKey:   stepDef.Key,
	}); err != nil {
		result.errorMessage = fmt.Sprintf("failed to mark step %s running: %v", stepDef.Key, err)
		_, _ = e.queries.RunStepUpdateForComplete(ctx, db.RunStepUpdateForCompleteParams{
			Status:       "failed",
			CompletedAt:  time.Now().UTC().Format(time.RFC3339Nano),
			DurationMs:   0,
			OutputJson:   "{}",
			ErrorMessage: result.errorMessage,
			LogExcerpt:   result.errorMessage,
			RunID:        runID,
			StepKey:      stepDef.Key,
		})
		_ = e.addEvent(ctx, runID, stepDef.Key, "step_failed", "error", result.errorMessage, map[string]any{"duration_ms": 0})
		return result
	}
	_ = e.addEvent(ctx, runID, stepDef.Key, "step_started", "info", "step started", map[string]any{})

	inputValue, err := hydrateTypedInput(stepDef, outputs)
	if err != nil {
		durationMs := time.Since(stepStart).Milliseconds()
		if durationMs < 0 {
			durationMs = 0
		}
		_, _ = e.queries.RunStepUpdateForComplete(ctx, db.RunStepUpdateForCompleteParams{
			Status:       "failed",
			CompletedAt:  time.Now().UTC().Format(time.RFC3339Nano),
			DurationMs:   durationMs,
			OutputJson:   "{}",
			ErrorMessage: err.Error(),
			LogExcerpt:   err.Error(),
			RunID:        runID,
			StepKey:      stepDef.Key,
		})
		_ = e.addEvent(ctx, runID, stepDef.Key, "step_failed", "error", err.Error(), map[string]any{"duration_ms": durationMs})
		result.errorMessage = err.Error()
		return result
	}

	stepLogger := e.newStepLogger(runID, jobKey, stepDef.Key, stepAttempt, os.Stderr)
	var partitionMetaCopy *RunPartitionMeta
	if partitionMeta != nil {
		partitionMetaCopy = cloneRunPartitionMeta(*partitionMeta)
	}
	stepCtx := WithRunMeta(ctx, RunMeta{
		RunID:     runID,
		JobKey:    jobKey,
		StepKey:   stepDef.Key,
		Attempt:   stepAttempt,
		Params:    copyParams(params),
		Partition: partitionMetaCopy,
	})
	stepCtx = WithLogger(stepCtx, stepLogger)

	var (
		outputValue reflect.Value
		stepErr     error
	)
	if captureGlobalLogs {
		previousDefaultLogger := slog.Default()
		stdioCapture, captureErr := startStdIOCapture(func(stream string, line string) {
			level := "info"
			if stream == "stderr" {
				level = "warn"
			}
			_ = e.addEvent(context.Background(), runID, stepDef.Key, "step_log", level, strings.TrimSpace(line), map[string]any{
				"source": "stdio",
				"stream": stream,
			})
		})
		if captureErr != nil {
			_ = e.addEvent(ctx, runID, stepDef.Key, "step_log", "warn", "failed to capture step stdio", map[string]any{
				"source": "executor",
				"error":  captureErr.Error(),
			})
		}
		slog.SetDefault(stepLogger)
		outputValue, stepErr = executeTypedStep(stepDef, stepCtx, inputValue)
		slog.SetDefault(previousDefaultLogger)
		if stdioCapture != nil {
			stdioCapture.Stop()
		}
	} else {
		outputValue, stepErr = executeTypedStep(stepDef, stepCtx, inputValue)
	}

	durationMs := time.Since(stepStart).Milliseconds()
	if durationMs < 0 {
		durationMs = 0
	}
	if stepErr != nil {
		_, _ = e.queries.RunStepUpdateForComplete(ctx, db.RunStepUpdateForCompleteParams{
			Status:       "failed",
			CompletedAt:  time.Now().UTC().Format(time.RFC3339Nano),
			DurationMs:   durationMs,
			OutputJson:   "{}",
			ErrorMessage: stepErr.Error(),
			LogExcerpt:   stepErr.Error(),
			RunID:        runID,
			StepKey:      stepDef.Key,
		})
		_ = e.addEvent(ctx, runID, stepDef.Key, "step_failed", "error", stepErr.Error(), map[string]any{"duration_ms": durationMs})
		result.errorMessage = stepErr.Error()
		return result
	}

	payload, err := json.Marshal(outputValue.Interface())
	if err != nil {
		marshalErr := fmt.Errorf("marshal output for step %s: %w", stepDef.Key, err)
		_, _ = e.queries.RunStepUpdateForComplete(ctx, db.RunStepUpdateForCompleteParams{
			Status:       "failed",
			CompletedAt:  time.Now().UTC().Format(time.RFC3339Nano),
			DurationMs:   durationMs,
			OutputJson:   "{}",
			ErrorMessage: marshalErr.Error(),
			LogExcerpt:   marshalErr.Error(),
			RunID:        runID,
			StepKey:      stepDef.Key,
		})
		_ = e.addEvent(ctx, runID, stepDef.Key, "step_failed", "error", marshalErr.Error(), map[string]any{"duration_ms": durationMs})
		result.errorMessage = marshalErr.Error()
		return result
	}

	_, _ = e.queries.RunStepUpdateForComplete(ctx, db.RunStepUpdateForCompleteParams{
		Status:       "success",
		CompletedAt:  time.Now().UTC().Format(time.RFC3339Nano),
		DurationMs:   durationMs,
		OutputJson:   string(payload),
		ErrorMessage: "",
		LogExcerpt:   "step succeeded",
		RunID:        runID,
		StepKey:      stepDef.Key,
	})
	_ = e.addEvent(ctx, runID, stepDef.Key, "step_succeeded", "info", "step succeeded", map[string]any{"duration_ms": durationMs})
	result.status = "success"
	result.output = payload
	return result
}

func cloneOutputs(outputs map[string]json.RawMessage, mu *sync.RWMutex) map[string]json.RawMessage {
	mu.RLock()
	defer mu.RUnlock()
	copied := make(map[string]json.RawMessage, len(outputs))
	for key, value := range outputs {
		copied[key] = value
	}
	return copied
}

func executeTypedStep(step StepDefinition, ctx context.Context, inputValue reflect.Value) (outputValue reflect.Value, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic in step %s: %v", step.Key, recovered)
		}
	}()
	outputValue, err = step.invoke(ctx, inputValue)
	return
}

func hydrateTypedInput(step StepDefinition, outputs map[string]json.RawMessage) (reflect.Value, error) {
	inputValue := step.newInputValue()
	if step.inputType == noInputType() {
		return inputValue, nil
	}

	for _, binding := range step.Bindings {
		field := inputValue.FieldByName(binding.FieldName)
		if !field.IsValid() {
			return reflect.Value{}, fmt.Errorf("invalid field %s for step input", binding.FieldName)
		}

		switch binding.Mode {
		case InputResolutionSingular:
			if len(binding.Providers) != 1 {
				return reflect.Value{}, fmt.Errorf("field %s expected exactly one provider, got %d", binding.FieldName, len(binding.Providers))
			}
			provider := binding.Providers[0]
			raw, ok := outputs[provider]
			if !ok {
				return reflect.Value{}, fmt.Errorf("missing output from dependency %s for field %s", provider, binding.FieldName)
			}
			target := reflect.New(field.Type())
			if err := json.Unmarshal(raw, target.Interface()); err != nil {
				return reflect.Value{}, fmt.Errorf("decode dependency %s into field %s: %w", provider, binding.FieldName, err)
			}
			field.Set(target.Elem())
		case InputResolutionPointer:
			if len(binding.Providers) > 1 {
				return reflect.Value{}, fmt.Errorf("field %s pointer input expected zero or one provider, got %d", binding.FieldName, len(binding.Providers))
			}
			if len(binding.Providers) == 0 {
				field.Set(reflect.Zero(field.Type()))
				continue
			}
			provider := binding.Providers[0]
			raw, ok := outputs[provider]
			if !ok {
				return reflect.Value{}, fmt.Errorf("missing output from dependency %s for field %s", provider, binding.FieldName)
			}
			target := reflect.New(field.Type().Elem())
			if err := json.Unmarshal(raw, target.Interface()); err != nil {
				return reflect.Value{}, fmt.Errorf("decode dependency %s into pointer field %s: %w", provider, binding.FieldName, err)
			}
			field.Set(target)
		case InputResolutionSlice:
			slice := reflect.MakeSlice(field.Type(), 0, len(binding.Providers))
			for _, provider := range binding.Providers {
				raw, ok := outputs[provider]
				if !ok {
					return reflect.Value{}, fmt.Errorf("missing output from dependency %s for slice field %s", provider, binding.FieldName)
				}
				target := reflect.New(field.Type().Elem())
				if err := json.Unmarshal(raw, target.Interface()); err != nil {
					return reflect.Value{}, fmt.Errorf("decode dependency %s into slice field %s: %w", provider, binding.FieldName, err)
				}
				slice = reflect.Append(slice, target.Elem())
			}
			field.Set(slice)
		default:
			return reflect.Value{}, fmt.Errorf("field %s has unsupported binding mode %q", binding.FieldName, binding.Mode)
		}
	}
	return inputValue, nil
}

func copyParams(params map[string]any) map[string]any {
	if len(params) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(params))
	for key, value := range params {
		out[key] = value
	}
	return out
}

func cloneRunPartitionMeta(meta RunPartitionMeta) *RunPartitionMeta {
	return &RunPartitionMeta{
		DefinitionID:  meta.DefinitionID,
		SelectionMode: meta.SelectionMode,
		Keys:          cloneStrings(meta.Keys),
		Ranges:        append([]PartitionSelectionRange(nil), meta.Ranges...),
		BackfillKey:   meta.BackfillKey,
	}
}

func normalizePartitionSelectionMode(raw string) PartitionSelectionMode {
	switch PartitionSelectionMode(strings.TrimSpace(raw)) {
	case PartitionSelectionModeSingle:
		return PartitionSelectionModeSingle
	case PartitionSelectionModeRange:
		return PartitionSelectionModeRange
	case PartitionSelectionModeSubset:
		return PartitionSelectionModeSubset
	default:
		return PartitionSelectionModeSubset
	}
}

func (e *Executor) loadRunPartitionMeta(ctx context.Context, runID int64) (*RunPartitionMeta, error) {
	if e == nil || runID <= 0 {
		return nil, nil
	}
	target, err := e.queries.RunPartitionTargetGetByRunID(ctx, runID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return runPartitionMetaFromTarget(target)
}

func runPartitionMetaFromTarget(target db.RunPartitionTarget) (*RunPartitionMeta, error) {
	mode := normalizePartitionSelectionMode(target.SelectionMode)
	keys, err := partitionKeysFromRunTarget(target)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}

	start := strings.TrimSpace(target.RangeStartKey)
	end := strings.TrimSpace(target.RangeEndKey)
	ranges := []PartitionSelectionRange{}
	switch mode {
	case PartitionSelectionModeSingle:
		ranges = []PartitionSelectionRange{{StartKey: keys[0], EndKey: keys[0]}}
	case PartitionSelectionModeRange:
		if start != "" && end != "" {
			ranges = []PartitionSelectionRange{{StartKey: start, EndKey: end}}
		}
		if len(ranges) == 0 {
			ranges = []PartitionSelectionRange{{StartKey: keys[0], EndKey: keys[len(keys)-1]}}
		}
	case PartitionSelectionModeSubset:
		if len(keys) == 1 {
			ranges = []PartitionSelectionRange{{StartKey: keys[0], EndKey: keys[0]}}
		}
	}

	return &RunPartitionMeta{
		DefinitionID:  target.PartitionDefinitionID,
		SelectionMode: mode,
		Keys:          keys,
		Ranges:        ranges,
		BackfillKey:   strings.TrimSpace(target.BackfillKey),
	}, nil
}

func partitionKeysFromRunTarget(target db.RunPartitionTarget) ([]string, error) {
	subsetJSON := strings.TrimSpace(target.PartitionSubsetJson)
	if subsetJSON != "" {
		var keys []string
		if err := json.Unmarshal([]byte(subsetJSON), &keys); err != nil {
			return nil, fmt.Errorf("parse run partition subset for run %d: %w", target.RunID, err)
		}
		normalized, err := normalizeOrderedKeys(keys, "partition key")
		if err != nil {
			return nil, err
		}
		return normalized, nil
	}

	partitionKey := strings.TrimSpace(target.PartitionKey)
	if partitionKey != "" {
		return []string{partitionKey}, nil
	}

	rangeStart := strings.TrimSpace(target.RangeStartKey)
	rangeEnd := strings.TrimSpace(target.RangeEndKey)
	if rangeStart != "" && rangeEnd != "" {
		if rangeStart == rangeEnd {
			return []string{rangeStart}, nil
		}
		return []string{rangeStart, rangeEnd}, nil
	}
	return []string{}, nil
}

func backfillPartitionOutcomeForRun(runStatus, runErrorMessage string) (string, string) {
	status := normalizeExecutionStatus(runStatus)
	if status == "success" {
		return backfillPartitionStatusDone, ""
	}
	message := strings.TrimSpace(runErrorMessage)
	if message == "" {
		switch status {
		case "canceled":
			message = "run canceled"
		default:
			message = "run failed"
		}
	}
	return backfillPartitionStatusFailed, message
}

func fallbackBackfillError(runErrorMessage string) string {
	message := strings.TrimSpace(runErrorMessage)
	if message != "" {
		return message
	}
	return "one or more backfill runs failed"
}

func (e *Executor) reconcileBackfillForRun(ctx context.Context, runID int64, runStatus, runErrorMessage string) error {
	if e == nil || runID <= 0 || e.queries == nil || e.pool == nil {
		return nil
	}

	tx, err := e.pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	qtx := db.WithTx(e.queries, tx)
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	target, err := qtx.RunPartitionTargetGetByRunID(ctx, runID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}

	backfillKey := strings.TrimSpace(target.BackfillKey)
	if backfillKey == "" {
		return nil
	}

	backfillRow, err := qtx.BackfillGetByKey(ctx, backfillKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}

	partitionStatus, partitionError := backfillPartitionOutcomeForRun(runStatus, runErrorMessage)
	partitionsByRun, err := qtx.BackfillPartitionGetManyByRunID(ctx, runID)
	if err != nil {
		return err
	}

	updatedKeys := make(map[string]struct{})
	for _, partition := range partitionsByRun {
		if partition.BackfillID != backfillRow.ID {
			continue
		}
		partitionKey := strings.TrimSpace(partition.PartitionKey)
		if partitionKey == "" {
			continue
		}
		if _, exists := updatedKeys[partitionKey]; exists {
			continue
		}
		if _, err := qtx.BackfillPartitionUpdateStatusByBackfillIDAndPartitionKey(ctx, db.BackfillPartitionUpdateStatusByBackfillIDAndPartitionKeyParams{
			Status:       partitionStatus,
			RunID:        runID,
			ErrorMessage: partitionError,
			BackfillID:   backfillRow.ID,
			PartitionKey: partitionKey,
		}); err != nil {
			return err
		}
		updatedKeys[partitionKey] = struct{}{}
	}

	if len(updatedKeys) == 0 {
		keys, keyErr := partitionKeysFromRunTarget(target)
		if keyErr != nil {
			return keyErr
		}
		for _, partitionKey := range keys {
			if _, exists := updatedKeys[partitionKey]; exists {
				continue
			}
			if _, err := qtx.BackfillPartitionUpdateStatusByBackfillIDAndPartitionKey(ctx, db.BackfillPartitionUpdateStatusByBackfillIDAndPartitionKeyParams{
				Status:       partitionStatus,
				RunID:        runID,
				ErrorMessage: partitionError,
				BackfillID:   backfillRow.ID,
				PartitionKey: partitionKey,
			}); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					continue
				}
				return err
			}
			updatedKeys[partitionKey] = struct{}{}
		}
	}

	materializedCount, err := qtx.BackfillPartitionCountByBackfillIDAndStatus(ctx, db.BackfillPartitionCountByBackfillIDAndStatusParams{
		BackfillID: backfillRow.ID,
		Status:     backfillPartitionStatusDone,
	})
	if err != nil {
		return err
	}
	failedCount, err := qtx.BackfillPartitionCountByBackfillIDAndStatus(ctx, db.BackfillPartitionCountByBackfillIDAndStatusParams{
		BackfillID: backfillRow.ID,
		Status:     backfillPartitionStatusFailed,
	})
	if err != nil {
		return err
	}
	totalTracked, err := qtx.BackfillPartitionCountByBackfillID(ctx, backfillRow.ID)
	if err != nil {
		return err
	}

	accounted := materializedCount + failedCount
	backfillStatus := backfillStatusRunning
	completedAt := ""
	errorMessage := ""
	if accounted >= totalTracked && totalTracked > 0 {
		completedAt = time.Now().UTC().Format(time.RFC3339Nano)
		if failedCount > 0 {
			backfillStatus = backfillStatusFailed
			errorMessage = fallbackBackfillError(runErrorMessage)
		} else {
			backfillStatus = backfillStatusCompleted
		}
	} else if failedCount > 0 {
		backfillStatus = backfillStatusRunning
		if strings.TrimSpace(backfillRow.ErrorMessage) != "" {
			errorMessage = backfillRow.ErrorMessage
		} else {
			errorMessage = fallbackBackfillError(runErrorMessage)
		}
	}

	startedAt := backfillRow.StartedAt
	if strings.TrimSpace(startedAt) == "" {
		startedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if _, err := qtx.BackfillUpdateStatus(ctx, db.BackfillUpdateStatusParams{
		Status:                  backfillStatus,
		RequestedPartitionCount: backfillRow.RequestedPartitionCount,
		RequestedRunCount:       backfillRow.RequestedRunCount,
		CompletedPartitionCount: materializedCount,
		FailedPartitionCount:    failedCount,
		ErrorMessage:            errorMessage,
		StartedAt:               startedAt,
		CompletedAt:             completedAt,
		ID:                      backfillRow.ID,
	}); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

func (e *Executor) failRun(ctx context.Context, runID int64, message string) error {
	run, err := e.queries.RunGetByID(ctx, runID)
	if err == nil && isTerminalExecutionStatus(run.Status) {
		return nil
	}
	_, err = e.queries.RunUpdateForComplete(ctx, db.RunUpdateForCompleteParams{
		Status:       "failed",
		CompletedAt:  time.Now().UTC().Format(time.RFC3339Nano),
		ErrorMessage: message,
		ID:           runID,
	})
	_ = e.addEvent(ctx, runID, "", "run_failed", "error", message, map[string]any{})
	if reconcileErr := e.reconcileBackfillForRun(ctx, runID, "failed", message); reconcileErr != nil {
		slog.Error("daggo: failed to reconcile backfill state after run failure", "run_id", runID, "err", reconcileErr)
	}
	return err
}

func (e *Executor) addEvent(ctx context.Context, runID int64, stepKey, eventType, level, message string, payload map[string]any) error {
	if payload == nil {
		payload = map[string]any{}
	}
	bytes, err := json.Marshal(payload)
	if err != nil {
		bytes = []byte("{}")
	}
	_, err = e.queries.RunEventCreate(ctx, db.RunEventCreateParams{
		RunID:         runID,
		StepKey:       stepKey,
		EventType:     eventType,
		Level:         level,
		Message:       message,
		EventDataJson: string(bytes),
	})
	return err
}

func (e *Executor) newStepLogger(runID int64, jobKey, stepKey string, attempt int64, terminal io.Writer) *slog.Logger {
	eventHandler := NewStepLogHandler(func(level string, message string, payload map[string]any) {
		_ = e.addEvent(context.Background(), runID, stepKey, "step_log", level, message, payload)
	})
	handlers := []slog.Handler{eventHandler}
	if terminal != nil {
		handlers = append(handlers, slog.NewTextHandler(terminal, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(NewMultiHandler(handlers...)).With(
		"run_id", runID,
		"job_key", jobKey,
		"step_key", stepKey,
		"attempt", attempt,
	)
}
