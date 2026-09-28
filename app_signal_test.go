//go:build unix

package daggo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/db"
)

const (
	signalFixtureSecret = "signal-fixture-secret"
	signalFixtureJobKey = "signal_fixture"
)

var (
	signalFixtureOnce   sync.Once
	signalFixtureBinary string
	signalFixtureErr    error
	signalFixtureDir    string
)

// signalFixture is one DAGGO server process, built from the repo as an
// imported app would be, running a job whose only step blocks until released.
type signalFixture struct {
	t       *testing.T
	dir     string
	dbPath  string
	baseURL string
	cmd     *exec.Cmd
	output  *bytes.Buffer
	exited  chan error
}

func buildSignalFixtureBinary(t *testing.T) string {
	t.Helper()

	signalFixtureOnce.Do(func() {
		repoRoot, err := os.Getwd()
		if err != nil {
			signalFixtureErr = err
			return
		}
		signalFixtureDir, err = os.MkdirTemp("", "daggo-signal-fixture-")
		if err != nil {
			signalFixtureErr = err
			return
		}
		goMod := fmt.Sprintf(`module daggo_signal_fixture

go 1.25

require github.com/swetjen/daggo v0.0.0

replace github.com/swetjen/daggo => %s
`, filepath.ToSlash(repoRoot))
		if err := os.WriteFile(filepath.Join(signalFixtureDir, "go.mod"), []byte(goMod), 0o644); err != nil {
			signalFixtureErr = err
			return
		}
		if err := os.WriteFile(filepath.Join(signalFixtureDir, "main.go"), []byte(signalFixtureMainSource), 0o644); err != nil {
			signalFixtureErr = err
			return
		}
		binaryPath := filepath.Join(signalFixtureDir, "signal-fixture")
		cmd := exec.Command("go", "build", "-mod=mod", "-o", binaryPath, ".")
		cmd.Dir = signalFixtureDir
		cmd.Env = append(os.Environ(), "GOWORK=off")
		if output, err := cmd.CombinedOutput(); err != nil {
			signalFixtureErr = fmt.Errorf("build signal fixture: %w\n%s", err, string(output))
			return
		}
		signalFixtureBinary = binaryPath
	})
	if signalFixtureErr != nil {
		t.Fatalf("signal fixture: %v", signalFixtureErr)
	}
	return signalFixtureBinary
}

func TestMain(m *testing.M) {
	code := m.Run()
	if signalFixtureDir != "" {
		_ = os.RemoveAll(signalFixtureDir)
	}
	os.Exit(code)
}

func freeLocalPort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func startSignalFixture(t *testing.T, graceSeconds int) *signalFixture {
	t.Helper()

	binary := buildSignalFixtureBinary(t)
	dir := t.TempDir()
	port := freeLocalPort(t)

	fixture := &signalFixture{
		t:       t,
		dir:     dir,
		dbPath:  filepath.Join(dir, "daggo.sqlite"),
		baseURL: "http://127.0.0.1:" + strconv.Itoa(port),
		output:  &bytes.Buffer{},
		exited:  make(chan error, 1),
	}
	fixture.cmd = exec.Command(binary)
	fixture.cmd.Dir = dir
	fixture.cmd.Env = append(os.Environ(),
		"FIXTURE_DIR="+dir,
		"FIXTURE_DB="+fixture.dbPath,
		"FIXTURE_PORT="+strconv.Itoa(port),
		"FIXTURE_SECRET="+signalFixtureSecret,
		"FIXTURE_GRACE_SECONDS="+strconv.Itoa(graceSeconds),
	)
	fixture.cmd.Stdout = fixture.output
	fixture.cmd.Stderr = fixture.output
	if err := fixture.cmd.Start(); err != nil {
		t.Fatalf("start signal fixture: %v", err)
	}
	go func() { fixture.exited <- fixture.cmd.Wait() }()

	t.Cleanup(func() {
		// Never leave a server or a worker behind, whatever the outcome.
		_ = fixture.cmd.Process.Kill()
		if pid, ok := fixture.workerPID(); ok {
			_ = syscall.Kill(-pid, syscall.SIGKILL)
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
		if t.Failed() {
			t.Logf("signal fixture output:\n%s", fixture.output.String())
		}
	})

	fixture.waitUntil(10*time.Second, "the server to report healthy", func() bool {
		status, body := fixture.health()
		return status == http.StatusOK && body["status"] == "ok"
	})
	return fixture
}

func (f *signalFixture) waitUntil(timeout time.Duration, description string, condition func() bool) {
	f.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		select {
		case err := <-f.exited:
			f.exited <- err
			f.t.Fatalf("server exited while waiting for %s: %v", description, err)
		case <-time.After(20 * time.Millisecond):
		}
	}
	f.t.Fatalf("timed out after %s waiting for %s", timeout, description)
}

// health calls the health endpoint without credentials.
func (f *signalFixture) health() (int, map[string]any) {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(f.baseURL + "/healthz")
	if err != nil {
		return 0, nil
	}
	defer resp.Body.Close()
	body := map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body
}

func (f *signalFixture) createRun() (int, map[string]any) {
	f.t.Helper()

	req, err := http.NewRequest(http.MethodPost, f.baseURL+"/rpc/runs/run-create", strings.NewReader(`{"job_key":"`+signalFixtureJobKey+`"}`))
	if err != nil {
		f.t.Fatalf("build run-create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+signalFixtureSecret)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		f.t.Fatalf("run-create: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	body := map[string]any{}
	_ = json.Unmarshal(raw, &body)
	return resp.StatusCode, body
}

func (f *signalFixture) startBlockingRun() int64 {
	f.t.Helper()

	status, body := f.createRun()
	if status != http.StatusOK {
		f.t.Fatalf("run-create = %d body=%v", status, body)
	}
	run, _ := body["run"].(map[string]any)
	id, _ := run["id"].(float64)
	if id <= 0 {
		f.t.Fatalf("run-create returned no run id: %v", body)
	}
	f.waitUntil(10*time.Second, "the worker to start the step", func() bool {
		_, ok := f.workerPID()
		return ok
	})
	return int64(id)
}

func (f *signalFixture) workerPID() (int, bool) {
	data, err := os.ReadFile(filepath.Join(f.dir, "step-started"))
	if err != nil {
		return 0, false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return 0, false
	}
	return pid, true
}

func (f *signalFixture) releaseStep() {
	f.t.Helper()
	if err := os.WriteFile(filepath.Join(f.dir, "step-release"), []byte("go"), 0o644); err != nil {
		f.t.Fatalf("release step: %v", err)
	}
}

func (f *signalFixture) signal(sig syscall.Signal) {
	f.t.Helper()
	if err := f.cmd.Process.Signal(sig); err != nil {
		f.t.Fatalf("send %s: %v", sig, err)
	}
}

// waitForExit returns the server's exit code.
func (f *signalFixture) waitForExit(timeout time.Duration) int {
	f.t.Helper()
	select {
	case err := <-f.exited:
		f.exited <- err
		if err == nil {
			return 0
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return exitErr.ExitCode()
		}
		f.t.Fatalf("wait for server: %v", err)
	case <-time.After(timeout):
		f.t.Fatalf("server did not exit within %s", timeout)
	}
	return -1
}

func signalProcessAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

type signalFixtureRun struct {
	run    db.Run
	steps  []db.RunStep
	events map[string]int
	stuck  int
}

// loadRun reads the run from the database after the server has exited.
func (f *signalFixture) loadRun(runID int64) signalFixtureRun {
	f.t.Helper()

	ctx := context.Background()
	queries, pool, err := db.Open(ctx, "file:"+f.dbPath+"?cache=shared&mode=rwc")
	if err != nil {
		f.t.Fatalf("open fixture db: %v", err)
	}
	defer pool.Close()

	out := signalFixtureRun{events: map[string]int{}}
	out.run, err = queries.RunGetByID(ctx, runID)
	if err != nil {
		f.t.Fatalf("load run %d: %v", runID, err)
	}
	out.steps, err = queries.RunStepGetManyByRunID(ctx, runID)
	if err != nil {
		f.t.Fatalf("load run steps: %v", err)
	}
	events, err := queries.RunEventGetManyByRunID(ctx, db.RunEventGetManyByRunIDParams{RunID: runID, Limit: 500, Offset: 0})
	if err != nil {
		f.t.Fatalf("load run events: %v", err)
	}
	for _, event := range events {
		out.events[event.EventType]++
	}
	if err := pool.QueryRowContext(ctx, "SELECT COUNT(*) FROM runs WHERE status IN ('running', 'queued')").Scan(&out.stuck); err != nil {
		f.t.Fatalf("count unfinished runs: %v", err)
	}
	return out
}

func TestServerSignalInterruptsRunsThatOutliveTheGracePeriod(t *testing.T) {
	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGINT} {
		sig := sig
		t.Run(sig.String(), func(t *testing.T) {
			t.Parallel()

			const graceSeconds = 2
			fixture := startSignalFixture(t, graceSeconds)
			runID := fixture.startBlockingRun()
			workerPID, _ := fixture.workerPID()
			if !signalProcessAlive(workerPID) {
				t.Fatalf("expected worker pid %d to be alive before the signal", workerPID)
			}

			signaledAt := time.Now()
			fixture.signal(sig)

			// While draining the server stays up, reports that it is
			// draining, and refuses new runs.
			fixture.waitUntil(2*time.Second, "the server to report draining", func() bool {
				status, body := fixture.health()
				return status == http.StatusOK && body["draining"] == true
			})
			if status, body := fixture.createRun(); status == http.StatusOK || !strings.Contains(fmt.Sprint(body["error"]), "new runs are temporarily blocked") {
				t.Fatalf("expected run-create to be refused while draining, got %d %v", status, body)
			}
			if !signalProcessAlive(workerPID) {
				t.Fatalf("worker was ended before the grace period was over")
			}

			exitCode := fixture.waitForExit(15 * time.Second)
			elapsed := time.Since(signaledAt)
			if exitCode != 0 {
				t.Fatalf("server exit code = %d, want 0", exitCode)
			}
			if elapsed < graceSeconds*time.Second {
				t.Fatalf("server exited after %s, before the %ds grace period was over", elapsed, graceSeconds)
			}

			deadline := time.Now().Add(3 * time.Second)
			for signalProcessAlive(workerPID) && time.Now().Before(deadline) {
				time.Sleep(20 * time.Millisecond)
			}
			if signalProcessAlive(workerPID) {
				t.Fatalf("worker pid %d outlived the server: it was orphaned, not terminated", workerPID)
			}

			got := fixture.loadRun(runID)
			if got.run.Status != "failed" {
				t.Fatalf("run status = %q, want failed", got.run.Status)
			}
			if got.run.ErrorMessage != dag.RunInterruptedReason {
				t.Fatalf("run error message = %q, want %q", got.run.ErrorMessage, dag.RunInterruptedReason)
			}
			if got.run.CompletedAt == "" {
				t.Fatalf("expected the interrupted run to have completed_at set")
			}
			if got.events[dag.RunInterruptedEventType] != 1 {
				t.Fatalf("expected one %s event, got events %v", dag.RunInterruptedEventType, got.events)
			}
			if got.stuck != 0 {
				t.Fatalf("%d run(s) were left running or queued after shutdown", got.stuck)
			}
			for _, step := range got.steps {
				if step.Status == "running" || step.Status == "pending" {
					t.Fatalf("step %s was left in status %q", step.StepKey, step.Status)
				}
			}
		})
	}
}

func TestServerSignalLetsInFlightRunsFinishWithinTheGracePeriod(t *testing.T) {
	t.Parallel()

	const graceSeconds = 30
	fixture := startSignalFixture(t, graceSeconds)
	runID := fixture.startBlockingRun()

	signaledAt := time.Now()
	fixture.signal(syscall.SIGTERM)
	fixture.waitUntil(2*time.Second, "the server to report draining", func() bool {
		status, body := fixture.health()
		return status == http.StatusOK && body["draining"] == true
	})

	// The run is still in flight, so the server must still be up.
	time.Sleep(500 * time.Millisecond)
	select {
	case err := <-fixture.exited:
		t.Fatalf("server exited while a run was in flight: %v", err)
	default:
	}

	fixture.releaseStep()
	exitCode := fixture.waitForExit(10 * time.Second)
	if exitCode != 0 {
		t.Fatalf("server exit code = %d, want 0", exitCode)
	}
	if elapsed := time.Since(signaledAt); elapsed >= graceSeconds*time.Second {
		t.Fatalf("server waited out the grace period (%s) instead of exiting once idle", elapsed)
	}

	got := fixture.loadRun(runID)
	if got.run.Status != "success" {
		t.Fatalf("run status = %q error=%q, want success", got.run.Status, got.run.ErrorMessage)
	}
	if got.events[dag.RunInterruptedEventType] != 0 {
		t.Fatalf("a run that finished in time must not be interrupted: %v", got.events)
	}
	if got.stuck != 0 {
		t.Fatalf("%d run(s) were left running or queued after shutdown", got.stuck)
	}
}

func TestServerSecondSignalEndsTheGracePeriodImmediately(t *testing.T) {
	t.Parallel()

	const graceSeconds = 120
	fixture := startSignalFixture(t, graceSeconds)
	runID := fixture.startBlockingRun()
	workerPID, _ := fixture.workerPID()

	fixture.signal(syscall.SIGTERM)
	fixture.waitUntil(2*time.Second, "the server to report draining", func() bool {
		status, body := fixture.health()
		return status == http.StatusOK && body["draining"] == true
	})
	signaledAt := time.Now()
	fixture.signal(syscall.SIGTERM)

	exitCode := fixture.waitForExit(10 * time.Second)
	if exitCode != 0 {
		t.Fatalf("server exit code = %d, want 0", exitCode)
	}
	if elapsed := time.Since(signaledAt); elapsed > 8*time.Second {
		t.Fatalf("second signal took %s to stop the server", elapsed)
	}
	deadline := time.Now().Add(3 * time.Second)
	for signalProcessAlive(workerPID) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if signalProcessAlive(workerPID) {
		t.Fatalf("worker pid %d outlived the server", workerPID)
	}

	got := fixture.loadRun(runID)
	if got.run.Status != "failed" || got.run.ErrorMessage != dag.RunInterruptedReason {
		t.Fatalf("run status=%q error=%q, want failed / %q", got.run.Status, got.run.ErrorMessage, dag.RunInterruptedReason)
	}
	if got.stuck != 0 {
		t.Fatalf("%d run(s) were left running or queued after shutdown", got.stuck)
	}
}

const signalFixtureMainSource = `package main

import (
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/swetjen/daggo"
	"github.com/swetjen/daggo/dag"
)

type output struct {
	Value string ` + "`json:\"value\"`" + `
}

func main() {
	dir := os.Getenv("FIXTURE_DIR")
	grace, err := strconv.Atoi(os.Getenv("FIXTURE_GRACE_SECONDS"))
	if err != nil {
		log.Fatal(err)
	}

	cfg := daggo.DefaultConfig()
	cfg.Admin.Port = os.Getenv("FIXTURE_PORT")
	cfg.Admin.SecretKey = os.Getenv("FIXTURE_SECRET")
	cfg.DisableUI = true
	cfg.Database.SQLite.Path = os.Getenv("FIXTURE_DB")
	cfg.Execution.Mode = "subprocess"
	cfg.Deploy.LockPath = filepath.Join(dir, "WILL_DEPLOY")
	cfg.Deploy.PollSeconds = 1
	cfg.Deploy.DrainGraceSeconds = grace

	step := dag.Op[dag.NoInput, output](
		"blocking_step",
		func(ctx context.Context, _ dag.NoInput) (output, error) {
			if err := os.WriteFile(filepath.Join(dir, "step-started"), []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
				return output{}, err
			}
			deadline := time.Now().Add(2 * time.Minute)
			for time.Now().Before(deadline) {
				if _, err := os.Stat(filepath.Join(dir, "step-release")); err == nil {
					return output{Value: "released"}, nil
				}
				time.Sleep(10 * time.Millisecond)
			}
			return output{}, errors.New("step was never released")
		},
	)
	job := dag.NewJob("signal_fixture").Add(step).MustBuild()

	if err := daggo.Run(context.Background(), cfg, job); err != nil {
		log.Fatal(err)
	}
}
`
