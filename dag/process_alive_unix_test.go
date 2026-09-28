//go:build unix

package dag

import (
	"errors"
	"syscall"
)

// processAlive reports whether a process with the pid still exists.
func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
