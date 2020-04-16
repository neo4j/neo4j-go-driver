package pool

import (
	"time"

	conn "github.com/neo4j/neo4j-go-driver/neo4j/internal/connection"
)

type fakeConn struct {
	serverName string
	isAlive    bool
}

func (c *fakeConn) TxBegin(mode conn.AccessMode, bookmarks []string, timeout time.Duration, meta map[string]interface{}) (conn.Handle, error) {
	return nil, nil
}
func (c *fakeConn) TxRollback(tx conn.Handle) error {
	return nil
}
func (c *fakeConn) TxCommit(tx conn.Handle) error {
	return nil
}
func (c *fakeConn) Run(
	cypher string, params map[string]interface{}, mode conn.AccessMode, bookmarks []string, timeout time.Duration, meta map[string]interface{}) (*conn.Stream, error) {

	return nil, nil
}
func (c *fakeConn) RunTx(tx conn.Handle, cypher string, params map[string]interface{}) (*conn.Stream, error) {
	return nil, nil
}
func (c *fakeConn) Next(s conn.Handle) (*conn.Record, *conn.Summary, error) {
	return nil, nil, nil
}
func (c *fakeConn) IsAlive() bool {
	return c.isAlive
}
func (c *fakeConn) ServerName() string {
	return c.serverName
}
func (c *fakeConn) Close() {
}
func (c *fakeConn) Reset() {
}
