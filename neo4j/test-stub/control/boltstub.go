/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package control

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// StubServer represents a running instance of a scripted bolt stub server
type StubServer struct {
	port            int
	script          string
	conn            net.Conn
	stub            *exec.Cmd
	stubExited      int32
	stubExitChannel chan string
	stubExitError   atomic.Value
}

const (
	connectionAttempts = 10
	exitAttempts       = 10
)

func (server *StubServer) markExited() {
	atomic.CompareAndSwapInt32(&server.stubExited, 0, 1)
}

func (server *StubServer) exited() bool {
	return atomic.LoadInt32(&server.stubExited) == 1
}

func (server *StubServer) markExitError(text string) {
	server.stubExitError.Store(text)
}

func (server *StubServer) exitError() string {
	value := server.stubExitError.Load()
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}

// NewStubServer launches the stub server on the given port with the given script
func NewStubServer(t *testing.T, port int, script string) *StubServer {
	var testScriptsDir = os.TempDir()

	if _, file, _, ok := runtime.Caller(1); ok {
		testScriptsDir = path.Join(path.Dir(file), "scripts")
	}

	if len(testScriptsDir) == 0 {
		t.Fatalf("unable to locate bolt stub script folder")
	}

	testScriptFile := path.Join(testScriptsDir, script)
	if _, err := os.Stat(testScriptFile); os.IsNotExist(err) {
		t.Fatalf("unable to locate bolt stub script file at '%s'", testScriptFile)
	}

	cmd := exec.Command("boltstub", fmt.Sprint(port), testScriptFile)

	server := &StubServer{
		port:            port,
		script:          testScriptFile,
		stub:            cmd,
		stubExited:      0,
		stubExitChannel: make(chan string),
		stubExitError:   atomic.Value{},
	}

	// set empty
	server.markExitError("")

	go func(channel chan string) {
		var cmdErr error
		var output []byte

		output, cmdErr = cmd.CombinedOutput()

		if cmdErr != nil {
			server.markExitError(fmt.Sprintf("command execution (%v) failed with error %s, output is %s", cmd.Args, cmdErr.Error(), output))
		} else {
			if cmd.ProcessState.Success() {
				server.markExitError("")
			} else {
				server.markExitError(fmt.Sprintf("command execution (%v) failed with output %s", cmd.Args, output))
			}
		}

		server.markExited()

		channel <- "done"
	}(server.stubExitChannel)

	// try to establish a connection to the stub server
	for i := 0; i < connectionAttempts && !server.exited(); i++ {
		if conn, err := net.Dial("tcp", fmt.Sprintf(":%d", server.port)); err == nil {
			server.conn = conn

			return server
		}

		time.Sleep(200 * time.Millisecond)
	}

	if server.exited() && server.exitError() != "" {
		t.Fatalf(server.exitError())
	}

	t.Fatalf("unable to open a connection to boltstub server at [:%d]", server.port)

	return nil
}

// Finished expects the stub server to already be exited returns whether it was exited
// with success code. If the process did not exit as expected, it returns false (or fails the test)
func (server *StubServer) Finished(t *testing.T) bool {
	// Close our initial connection to make the stub server exit
	if server.conn != nil {
		server.conn.Close()
	}

	// wait for the stub process to exit
	for i := 0; i < exitAttempts; i++ {
		if server.exited() {
			break
		}

		time.Sleep(200 * time.Millisecond)
	}

	// Terminate if it's still running
	if !server.exited() {
		server.stub.Process.Kill()
	}

	// Wait for exit to complete
	<-server.stubExitChannel

	// Check if an error occurred
	if server.exitError() != "" {
		t.Fatalf(server.exitError())
	}

	return true
}

// Close releases all process related resources
func (server *StubServer) Close() {
	server.stub.Process.Release()
}
