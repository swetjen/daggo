//go:build !unix

package dag

import (
	"errors"
	"os"
	"os/exec"
)

func configureWorkerProcess(_ *exec.Cmd) {}

// killWorkerProcess terminates a worker process.
func killWorkerProcess(pid int) error {
	if pid <= 0 {
		return errors.New("worker pid is required")
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	if err := proc.Kill(); err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			return nil
		}
		return err
	}
	return nil
}
