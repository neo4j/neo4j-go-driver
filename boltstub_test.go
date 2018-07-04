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

package neo4j

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"runtime"
	"testing"
	"time"
)

type stubServer struct {
	test    *testing.T
	port    int
	script  string
	conn    net.Conn
	process *exec.Cmd
}

const (
	connectionAttempts = 10
)

var (
	testDataDir = ""
)

func startStubServer(t *testing.T, port int, script string) *stubServer {
	if len(testDataDir) == 0 {
		t.Fatalf("unable to locate bolt stub script folder")
	}

	testScriptFile := path.Join(testDataDir, script)
	if _, err := os.Stat(testScriptFile); os.IsNotExist(err) {
		t.Fatalf("unable to locate bolt stub script file at '%s'", testScriptFile)
	}

	cmd := exec.Command("boltstub", fmt.Sprint(port), testScriptFile)
	if err := cmd.Start(); err != nil {
		t.Fatalf("unable to start boltstub: %g", err)
	}

	server := &stubServer{test: t, port: port, script: testScriptFile, process: cmd}

	// try to establish a connection to the stub server
	for i := 0; i < connectionAttempts; i++ {
		conn, err := net.Dial("tcp", fmt.Sprintf(":%d", server.port))
		if err != nil {
			time.Sleep(200 * time.Millisecond)
		}

		server.conn = conn

		return server
	}

	server.test.Fatalf("unable to open a connection to boltstub server at [:%d]", server.port)

	return nil
}

func (server *stubServer) waitForExit() bool {
	if err := server.process.Wait(); err != nil {
		server.test.Fatalf("unable to wait for boltstub server to exit: %g", err)
	}

	if server.process.ProcessState.Exited() {
		return server.process.ProcessState.Success()
	}

	if err := server.process.Process.Kill(); err != nil {
		server.test.Fatalf("unable to kill boltstub server: %g", err)
	}

	server.test.Fatalf("manually killed boltstub server")

	return false
}

func init() {
	if _, file, _, ok := runtime.Caller(1); ok {
		testDataDir = path.Join(path.Dir(file), "boltstub-scripts")
	}
}
