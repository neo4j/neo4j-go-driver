package neo4j_go_driver

import (
	"net"
	"bufio"
	"fmt"
)

// TODO: replace with the C connector

type Connection struct {
	status  int
	address string
	socket  net.Conn
	reader  *bufio.Reader
	writer  *bufio.Writer
	inData  []byte
	ProtocolVersion uint32
}

func Connect(address string) (Connection, error) {
	conn := Connection{
		address: address,
	}
	socket, err := net.Dial("tcp", address)
	if err != nil {
		return conn, err
	}
	conn.socket = socket
	conn.reader = bufio.NewReader(conn.socket)
	conn.writer = bufio.NewWriter(conn.socket)
	conn.inData = make([]byte, 4096)
	conn.ProtocolVersion, err = conn.handshake()
	if err != nil {
		conn.socket.Close()
		return conn, err
	}
	fmt.Printf("Using Bolt v%d\n", conn.ProtocolVersion)
	return conn, nil
}

func (conn Connection) handshake() (uint32, error) {
	_, err := conn.writer.Write([]byte("\x60\x60\xB0\x17\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00"))
	conn.writer.Flush()
	if err != nil {
		return 0, err
	}
	_, err = conn.reader.Read(conn.inData)
	return (uint32)(conn.inData[0] << 24 | conn.inData[1] << 16 | conn.inData[2] << 8 | conn.inData[3]), nil
}
