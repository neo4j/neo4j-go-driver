package main

import (
	"bufio"
	"net"
)

func main() {
	l, err := net.Listen("tcp", ":9876")
	if err != nil {
		panic(err)
	}

	for {
		// Wait for a testkit frontend connection, no need to bother handling more than one
		// frontend connection at the time.
		conn, err := l.Accept()
		if err != nil {
			panic(err)
		}
		// Hand over connection to backend
		backend := newBackend(bufio.NewReader(conn), bufio.NewWriter(conn))
		backend.serve()
	}
}
