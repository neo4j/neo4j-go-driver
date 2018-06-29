package neo4j_go_driver

import (
	"runtime"
	"path"
	"os"
	"testing"
	"os/exec"
	"net"
	"fmt"
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
