package daggo

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/db"
)

func TestCurrentProcessFromArgs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		args    []string
		want    ProcessInfo
		wantErr bool
	}{
		{
			name: "server",
			args: []string{"serve"},
			want: ProcessInfo{Mode: ProcessModeServer},
		},
		{
			name: "worker",
			args: []string{"daggo-worker", "--run-id", "42"},
			want: ProcessInfo{Mode: ProcessModeWorker, RunID: 42},
		},
		{
			name:    "worker requires run id",
			args:    []string{"daggo-worker"},
			wantErr: true,
		},
		{
			name:    "worker rejects invalid run id",
			args:    []string{"daggo-worker", "--run-id", "0"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := currentProcessFromArgs(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("current process: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestWorkerProcessSkipsGuardedServerStartup(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "daggo.sqlite")
	markerPath := filepath.Join(tempDir, "server-startup-ran")

	binaryPath := buildWorkerFixtureBinary(t, tempDir)

	cfg := DefaultConfig()
	cfg.Database.SQLite.Path = dbPath

	queries, pool, err := db.OpenRuntime(ctx, cfg.Database)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}

	registry := dag.NewRegistry()
	if err := registry.Register(workerStartupGuardFixtureJob()); err != nil {
		t.Fatalf("register fixture job: %v", err)
	}
	if err := registry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("sync fixture job: %v", err)
	}

	jobRow, err := queries.JobGetByKey(ctx, "worker_startup_guard_fixture")
	if err != nil {
		t.Fatalf("load fixture job: %v", err)
	}
	run, _, err := dag.InsertQueuedRun(ctx, queries, dag.RunInsertInput{
		JobID:       jobRow.ID,
		TriggeredBy: "worker-fixture-test",
	})
	if err != nil {
		t.Fatalf("create fixture run: %v", err)
	}
	if err := pool.Close(); err != nil {
		t.Fatalf("close setup db: %v", err)
	}

	cmd := exec.Command(binaryPath, "daggo-worker", "--run-id", strconv.FormatInt(run.ID, 10))
	cmd.Env = append(os.Environ(),
		"DAGGO_FIXTURE_DB="+dbPath,
		"DAGGO_SIDE_EFFECT_MARKER="+markerPath,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("worker fixture failed: %v\n%s", err, string(output))
	}
	if _, err := os.Stat(markerPath); !os.IsNotExist(err) {
		t.Fatalf("server-only startup marker should not exist, stat err=%v", err)
	}

	queries, pool, err = db.OpenRuntime(ctx, cfg.Database)
	if err != nil {
		t.Fatalf("reopen runtime: %v", err)
	}
	defer pool.Close()

	runRow, err := queries.RunGetByID(ctx, run.ID)
	if err != nil {
		t.Fatalf("load fixture run: %v", err)
	}
	if runRow.Status != "success" {
		t.Fatalf("expected run success, got status=%q error=%q", runRow.Status, runRow.ErrorMessage)
	}
}

type workerStartupGuardOutput struct {
	Value string `json:"value"`
}

func workerStartupGuardFixtureJob() dag.JobDefinition {
	step := dag.Op[dag.NoInput, workerStartupGuardOutput](
		"guarded_step",
		func(context.Context, dag.NoInput) (workerStartupGuardOutput, error) {
			return workerStartupGuardOutput{Value: "ok"}, nil
		},
	)
	return dag.NewJob("worker_startup_guard_fixture").Add(step).MustBuild()
}

func buildWorkerFixtureBinary(t *testing.T, tempDir string) string {
	t.Helper()

	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("get wd: %v", err)
	}
	fixtureDir := filepath.Join(tempDir, "fixture")
	if err := os.MkdirAll(fixtureDir, 0o755); err != nil {
		t.Fatalf("create fixture dir: %v", err)
	}

	goMod := fmt.Sprintf(`module daggo_worker_fixture

go 1.25

require github.com/swetjen/daggo v0.0.0

replace github.com/swetjen/daggo => %s
`, filepath.ToSlash(repoRoot))
	if err := os.WriteFile(filepath.Join(fixtureDir, "go.mod"), []byte(goMod), 0o644); err != nil {
		t.Fatalf("write fixture go.mod: %v", err)
	}
	if err := os.WriteFile(filepath.Join(fixtureDir, "main.go"), []byte(workerFixtureMainSource), 0o644); err != nil {
		t.Fatalf("write fixture main.go: %v", err)
	}

	binaryPath := filepath.Join(tempDir, "worker-fixture")
	cmd := exec.Command("go", "build", "-mod=mod", "-o", binaryPath, ".")
	cmd.Dir = fixtureDir
	cmd.Env = append(os.Environ(), "GOWORK=off")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build worker fixture: %v\n%s", err, string(output))
	}
	return binaryPath
}

const workerFixtureMainSource = `package main

import (
	"context"
	"log"
	"os"

	"github.com/swetjen/daggo"
	"github.com/swetjen/daggo/dag"
)

type output struct {
	Value string ` + "`json:\"value\"`" + `
}

func main() {
	cfg := daggo.DefaultConfig()
	cfg.Database.SQLite.Path = os.Getenv("DAGGO_FIXTURE_DB")

	job := fixtureJob()
	process, err := daggo.CurrentProcess()
	if err != nil {
		log.Fatal(err)
	}
	if process.Mode == daggo.ProcessModeServer {
		if err := os.WriteFile(os.Getenv("DAGGO_SIDE_EFFECT_MARKER"), []byte("ran"), 0o644); err != nil {
			log.Fatal(err)
		}
	}

	if err := daggo.Run(context.Background(), cfg, job); err != nil {
		log.Fatal(err)
	}
}

func fixtureJob() dag.JobDefinition {
	step := dag.Op[dag.NoInput, output](
		"guarded_step",
		func(context.Context, dag.NoInput) (output, error) {
			return output{Value: "ok"}, nil
		},
	)
	return dag.NewJob("worker_startup_guard_fixture").Add(step).MustBuild()
}
`
