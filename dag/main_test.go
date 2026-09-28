package dag

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// testWorkerCommand makes the test binary act as a stand-in run worker, so
// executor tests can exercise real worker processes. The stand-in records that
// it started, then stays alive until the test releases it.
const (
	testWorkerCommand = "daggo-test-worker"
	testWorkerDirEnv  = "DAGGO_TEST_WORKER_DIR"
)

func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == testWorkerCommand {
		os.Exit(runTestWorker(os.Args[2:]))
	}
	os.Exit(m.Run())
}

func runTestWorker(args []string) int {
	runID := ""
	for idx := 0; idx < len(args)-1; idx++ {
		if args[idx] == "--run-id" {
			runID = args[idx+1]
		}
	}
	dir := os.Getenv(testWorkerDirEnv)
	if runID == "" || dir == "" {
		fmt.Fprintln(os.Stderr, "test worker requires --run-id and "+testWorkerDirEnv)
		return 2
	}
	if err := os.WriteFile(filepath.Join(dir, "started-"+runID), []byte(fmt.Sprint(os.Getpid())), 0o644); err != nil {
		return 2
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(dir, "release-"+runID)); err == nil {
			return 0
		}
		time.Sleep(5 * time.Millisecond)
	}
	return 3
}
