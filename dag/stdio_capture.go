package dag

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

type stdioLineSink func(stream string, line string)

type stdioCapture struct {
	oldStdout *os.File
	oldStderr *os.File

	stdoutReader *os.File
	stdoutWriter *os.File
	stderrReader *os.File
	stderrWriter *os.File

	sink stdioLineSink
	wg   sync.WaitGroup
}

var stdioCaptureMu sync.Mutex

func startStdIOCapture(sink stdioLineSink) (*stdioCapture, error) {
	stdioCaptureMu.Lock()

	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		stdioCaptureMu.Unlock()
		return nil, err
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		_ = stdoutReader.Close()
		_ = stdoutWriter.Close()
		stdioCaptureMu.Unlock()
		return nil, err
	}

	capture := &stdioCapture{
		oldStdout:    os.Stdout,
		oldStderr:    os.Stderr,
		stdoutReader: stdoutReader,
		stdoutWriter: stdoutWriter,
		stderrReader: stderrReader,
		stderrWriter: stderrWriter,
		sink:         sink,
	}

	capture.wg.Add(2)
	go capture.forward("stdout", capture.stdoutReader, capture.oldStdout)
	go capture.forward("stderr", capture.stderrReader, capture.oldStderr)

	os.Stdout = capture.stdoutWriter
	os.Stderr = capture.stderrWriter

	return capture, nil
}

func (c *stdioCapture) Stop() {
	if c == nil {
		return
	}

	os.Stdout = c.oldStdout
	os.Stderr = c.oldStderr

	_ = c.stdoutWriter.Close()
	_ = c.stderrWriter.Close()
	c.wg.Wait()
	_ = c.stdoutReader.Close()
	_ = c.stderrReader.Close()

	stdioCaptureMu.Unlock()
}

func (c *stdioCapture) forward(stream string, reader *os.File, passthrough *os.File) {
	defer c.wg.Done()
	if reader == nil {
		return
	}
	buffered := bufio.NewReader(reader)
	for {
		line, err := buffered.ReadString('\n')
		if line != "" {
			trimmed := strings.TrimSpace(line)
			if passthrough != nil {
				_, _ = fmt.Fprint(passthrough, line)
			}
			if trimmed != "" && c.sink != nil {
				c.sink(stream, strings.TrimRight(line, "\r\n"))
			}
		}
		if err == nil {
			continue
		}
		if err == io.EOF {
			return
		}
		return
	}
}
