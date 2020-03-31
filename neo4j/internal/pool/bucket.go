package pool

// Not thread safe

import (
	conn "github.com/neo4j/neo4j-go-driver/neo4j/internal/connection"
)

type bucket struct {
	ready []conn.Connection
	size  int
}

func (b *bucket) get() conn.Connection {
	l := len(b.ready)
	if l == 0 {
		return nil
	}
	// Pop the last one, faster to just pop the end of the slice
	c := b.ready[l-1]
	b.ready = b.ready[:l-1]
	return c
}

func (b *bucket) ret(c conn.Connection) {
	b.ready = append(b.ready, c)
}

func (b bucket) num() int {
	return len(b.ready)
}

func (b *bucket) reg(c conn.Connection) {
	b.size += 1
}

func (b *bucket) unreg(c conn.Connection) {
	b.size -= 1
}
