/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"runtime"
	"time"

	. "github.com/onsi/ginkgo"
)

// StubServer represents a running instance of a scripted bolt stub server
type StubServer struct {
	port            int
	script          string
	conn            net.Conn
	stub            *exec.Cmd
	stubExited      bool
	stubExitChannel chan string
	stubExitError   error
}

const (
	connectionAttempts = 10
)

// NewStubServer launches the stub server on the given port with the given script
func NewStubServer(port int, script string) *StubServer {
	var testScriptsDir string = os.TempDir()

	if _, file, _, ok := runtime.Caller(1); ok {
		testScriptsDir = path.Join(path.Dir(file), "scripts")
	}

	if len(testScriptsDir) == 0 {
		Fail("unable to locate bolt stub script folder")
	}

	testScriptFile := path.Join(testScriptsDir, script)
	if _, err := os.Stat(testScriptFile); os.IsNotExist(err) {
		Fail(fmt.Sprintf("unable to locate bolt stub script file at '%s'", testScriptFile))
	}

	var stdoutBuf, stderrBuf bytes.Buffer
	var cmdErr error
	cmd := exec.Command("boltstub", fmt.Sprint(port), testScriptFile)
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	server := &StubServer{
		port:            port,
		script:          testScriptFile,
		stub:            cmd,
		stubExitChannel: make(chan string),
		stubExited:      false,
		stubExitError:   nil,
	}

	go func(channel chan string) {
		cmdErr = cmd.Run()

		server.stubExited = true

		if cmdErr != nil {
			server.stubExitError = fmt.Errorf("command execution (%v) failed with error %s", cmd.Args, cmdErr.Error())
		} else {
			if cmd.ProcessState.Success() {
				server.stubExitError = nil
			} else {
				server.stubExitError = fmt.Errorf("command execution (%v) failed with error %s", cmd.Args, stderrBuf.String())
			}
		}

		channel <- "done"
	}(server.stubExitChannel)

	// try to establish a connection to the stub server
	for i := 0; i < connectionAttempts && cmdErr == nil; i++ {
		if conn, err := net.Dial("tcp", fmt.Sprintf(":%d", server.port)); err == nil {
			server.conn = conn

			return server
		}

		time.Sleep(200 * time.Millisecond)
	}

	if server.stubExited && server.stubExitError != nil {
		Fail(server.stubExitError.Error())
	}

	Fail(fmt.Sprintf("unable to open a connection to boltstub server at [:%d]", server.port))

	return nil
}

// Finished expects the stub server to already be exited returns whether it was exited
// with success code. If the process did not exit as expected, it returns false (or fails the test)
func (server *StubServer) Finished() bool {
	// Close our initial connection to make the stub server exit
	if server.conn != nil {
		server.conn.Close()
	}

	// Wait for some time for the boltstub to exit
	time.Sleep(500 * time.Millisecond)

	// Terminate if it's still running
	if !server.stubExited {
		server.stub.Process.Kill()
	}

	// Wait for exit to complete
	<-server.stubExitChannel

	// Check if an error occurred
	if server.stubExitError != nil {
		Fail(server.stubExitError.Error())
	}

	return true
}

func (server *StubServer) Close() {
	if !server.stubExited {
		server.stub.Process.Kill()
	}
}