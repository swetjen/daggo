package daggo

import "os"

type ProcessMode string

const (
	// ProcessModeServer is the long-lived admin/scheduler process.
	ProcessModeServer ProcessMode = "server"
	// ProcessModeWorker is a DAGGO subprocess executing one run.
	ProcessModeWorker ProcessMode = "worker"
)

// ProcessInfo describes the role of the current binary invocation.
type ProcessInfo struct {
	Mode  ProcessMode
	RunID int64
}

// CurrentProcess reports whether the current binary invocation is the DAGGO
// server process or an internal worker subprocess.
func CurrentProcess() (ProcessInfo, error) {
	return currentProcessFromArgs(os.Args[1:])
}

func currentProcessFromArgs(args []string) (ProcessInfo, error) {
	handled, runID, err := maybeParseWorkerCommand(args)
	if err != nil {
		return ProcessInfo{}, err
	}
	if handled {
		return ProcessInfo{Mode: ProcessModeWorker, RunID: runID}, nil
	}
	return ProcessInfo{Mode: ProcessModeServer}, nil
}
