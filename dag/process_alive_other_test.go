//go:build !unix

package dag

import "os"

// processAlive reports whether a process with the pid still exists.
func processAlive(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	_ = proc.Release()
	return true
}
