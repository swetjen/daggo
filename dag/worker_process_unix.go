//go:build unix

package dag

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

// configureWorkerProcess puts the worker in its own process group. A signal
// sent to the server's process group (for example Ctrl+C in a terminal) then
// reaches only the server, which drains its workers, and a worker can be
// terminated together with any processes it spawned.
func configureWorkerProcess(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}

// killWorkerProcess terminates a worker and the processes in its group.
func killWorkerProcess(pid int) error {
	if pid <= 0 {
		return errors.New("worker pid is required")
	}
	groupErr := syscall.Kill(-pid, syscall.SIGKILL)
	if groupErr == nil {
		return nil
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
