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

package drivertest

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	"net"
	"os"
	"os/exec"
	"path"
	"time"
)

// StubServer represents a running instance of a scripted bolt stub server
type StubServer struct {
	port    int
	script  string
	conn    net.Conn
	process *exec.Cmd
}

const (
	connectionAttempts = 10
)

// NewStubServer launches the stub server on the given port with the given script
func NewStubServer(port int, script string) *StubServer {
	testScriptsDir := path.Join(GetExecutingFilesDir(), "scripts")

	if len(testScriptsDir) == 0 {
		Fail("unable to locate bolt stub script folder")
	}

	testScriptFile := path.Join(testScriptsDir, script)
	if _, err := os.Stat(testScriptFile); os.IsNotExist(err) {
		Fail(fmt.Sprintf("unable to locate bolt stub script file at '%s'", testScriptFile))
	}

	cmd := exec.Command("boltstub", fmt.Sprint(port), testScriptFile)
	if err := cmd.Start(); err != nil {
		Fail(fmt.Sprintf("unable to start boltstub: %g", err))
	}

	server := &StubServer{port: port, script: testScriptFile, process: cmd}

	// try to establish a connection to the stub server
	for i := 0; i < connectionAttempts; i++ {
		conn, err := net.Dial("tcp", fmt.Sprintf(":%d", server.port))
		if err != nil {
			time.Sleep(200 * time.Millisecond)
		}

		server.conn = conn

		return server
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

	if err := server.process.Wait(); err != nil {
		Fail(fmt.Sprintf("unable to wait for boltstub server to exit: %g", err))
	}

	if server.process.ProcessState.Exited() {
		return server.process.ProcessState.Success()
	}

	return false
}

func (server *StubServer) Close() {
	server.process.Process.Kill()
}
