package pool

// Thread safe

import (
	"container/list"
	"context"
	"errors"
	"sync"

	conn "github.com/neo4j/neo4j-go-driver/neo4j/internal/connection"
)

type Connect func(string) (conn.Connection, error)

type qitem struct {
	servers []string
	wakeup  chan bool
	conn    conn.Connection
}

type Pool struct {
	maxSize    int
	connect    Connect
	buckets    map[string]*bucket
	bucketsMut sync.Mutex
	queueMut   sync.Mutex
	queue      list.List
}

func New(maxSize int, connect Connect) *Pool {
	p := &Pool{
		maxSize: maxSize,
		connect: connect,
		buckets: make(map[string]*bucket),
	}
	return p
}

func log(format string, a ...interface{}) {
}

func (p *Pool) Close() {
	// Cancel everything in the queue by just emptying at and let all callers timeout
	p.queueMut.Lock()
	p.queue.Init()
	p.queueMut.Unlock()
	// Go through each bucket and close the connections in it
	p.bucketsMut.Lock()
	for n, b := range p.buckets {
		for c := b.get(); c != nil; c = b.get() {
			c.Close()
		}
		delete(p.buckets, n)
	}
	p.bucketsMut.Unlock()
}

func (p *Pool) tryExistingBucketsExistingConn(servers []string, checkTimeout func() error) (conn.Connection, error) {
	p.bucketsMut.Lock()
	defer p.bucketsMut.Unlock()

	for _, s := range servers {
		b := p.buckets[s]
		if b == nil {
			continue
		}
		// Try to get a free alive connection
		for {
			c := b.get()
			if c == nil {
				break
			}
			// Check that the connection is ok
			if !c.IsAlive() {
				// Unregister the connection and close it another thread to avoid potential
				// long blocking operation during close.
				log("%s: conn dead\n", s)
				b.unreg(c)
				go func() {
					c.Close()
				}()
				continue
			}
			log("%s: conn found\n", s)
			return c, nil
		}
		if b.size == 0 {
			log("%s: bucket dead\n", s)
			// Dead bucket, remove it
			delete(p.buckets, s)
		}
	}

	return nil, checkTimeout()
}

func (p *Pool) tryExistingBucketsNewConn(servers []string, checkTimeout func() error) (conn.Connection, error) {
	p.bucketsMut.Lock()
	defer p.bucketsMut.Unlock()

	for _, s := range servers {
		if err := checkTimeout(); err != nil {
			return nil, err
		}

		b := p.buckets[s]
		if b == nil {
			continue
		}
		if b.size >= p.maxSize {
			continue
		}
		// Try to connect, this might take a while
		log("%s: connecting\n", s)
		c, err := p.connect(s)
		if err != nil || c == nil {
			continue
		}
		// Register the connection in the bucket
		log("%s: conn registered\n", s)
		b.reg(c)
		return c, nil
	}
	return nil, nil
}

func (p *Pool) tryNewBucketNewConn(servers []string, checkTimeout func() error) (conn.Connection, error) {
	p.bucketsMut.Lock()
	defer p.bucketsMut.Unlock()

	for _, s := range servers {
		if err := checkTimeout(); err != nil {
			return nil, err
		}

		b := p.buckets[s]
		if b != nil {
			continue
		}

		// No bucket for this server, try to connect and add a bucket
		log("%s: bucket pending, connecting\n", s)
		c, err := p.connect(s)
		if err != nil || c == nil {
			// Blacklist this server for a while ?
			log("%s: bucket cancelled\n", s)
			continue
		}
		// Ok, got a connection, now create a bucket for this server and register the
		// connection within it
		b = &bucket{}
		p.buckets[s] = b
		log("%s: bucket added\n", s)
		b.reg(c)
		log("%s: conn registered\n", s)
		return c, nil
	}
	return nil, nil
}

func (p *Pool) anyExistingConnInExistingBuckets(servers []string) bool {
	p.bucketsMut.Lock()
	defer p.bucketsMut.Unlock()
	for _, s := range servers {
		b := p.buckets[s]
		if b != nil {
			if b.size > 0 {
				return true
			}
		}
	}
	return false
}

// For testing
func (p *Pool) queueSize() int {
	p.queueMut.Lock()
	defer p.queueMut.Unlock()
	return p.queue.Len()
}

// For testing
func (p *Pool) getBuckets() map[string]bucket {
	p.bucketsMut.Lock()
	defer p.bucketsMut.Unlock()
	buckets := make(map[string]bucket)
	for k, v := range p.buckets {
		buckets[k] = *v
	}
	return buckets
}

func (p *Pool) Borrow(ctx context.Context, servers []string) (conn.Connection, error) {
	check := func() error {
		select {
		case <-ctx.Done():
			log("time out\n")
			return ctx.Err()
		default:
			return nil
		}
	}

	log("Borrow %s\n", servers)

	// Try to use an existing connection in an existing bucket, that is cheapest.
	// This will also prune dead connections and empty buckets (servers with no connections) among
	// the requested ones.
	c, err := p.tryExistingBucketsExistingConn(servers, check)
	if err != nil || c != nil {
		return c, err
	}
	// The existing buckets at this point are "proved" to be working, at least we don't know that
	// they are down yet. So try to utilize these before trying another server and corresponding
	// new bucket.
	c, err = p.tryExistingBucketsNewConn(servers, check)
	if err != nil || c != nil {
		return c, err
	}
	// Try to increase size of any matching existing connection bucket. A non-functional server
	// would have it's bucket pruned above but retried to connect to here, could be good if the
	// server went down and now is up again but could also be bad if it is still down.
	c, err = p.tryNewBucketNewConn(servers, check)
	if err != nil || c != nil {
		return c, err
	}

	// If there are no buckets for any of the servers, there is no point in waiting for anything
	// to be returned.
	if !p.anyExistingConnInExistingBuckets(servers) {
		return nil, errors.New("Unable to connect")
	}

	// Wait for a matching connection to be returned from another thread.
	p.queueMut.Lock()
	// Ok, now that we own the queue we can add the item there but between getting the lock
	// and above check for an existing connection another thread might have returned a connection
	// so check again to avoid potentially starving this thread.
	c, err = p.tryExistingBucketsExistingConn(servers, check)
	if err != nil || c != nil {
		p.queueMut.Unlock()
		return c, err
	}
	// Add a waiting request to the queue and unlock the queue to let other threads that returns
	// their connections access the queue.
	q := &qitem{
		servers: servers,
		wakeup:  make(chan bool),
	}
	e := p.queue.PushBack(q)
	p.queueMut.Unlock()

	log("in queue\n")

	// Wait for either a wake up signal that indicates that we got a connection or a timeout.
	select {
	case <-q.wakeup:
		// Element removed by other thread
		log("woke up, got a conn\n")
		return q.conn, nil
	case <-ctx.Done():
		log("timed out, checking queue")
		p.queueMut.Lock()
		p.queue.Remove(e)
		p.queueMut.Unlock()
		if q.conn != nil {
			log("got a conn, recovering\n")
			return q.conn, nil
		}
		return nil, ctx.Err()
	}
}

func (p *Pool) unreg(server string, c conn.Connection) {
	p.bucketsMut.Lock()
	defer p.bucketsMut.Unlock()

	bucket := p.buckets[server]
	// Check for strange condition of not finding the bucket.
	if bucket != nil {
		bucket.unreg(c)
		if bucket.size == 0 {
			delete(p.buckets, server)
		}
	}

	// Close connection another thread to avoid potential long blocking operation during close.
	go func() {
		c.Close()
	}()
}

func (p *Pool) Return(c conn.Connection) {
	// Get the name of the bucket that the connection belongs to.
	server := c.ServerName()

	log("Return conn in %s\n", server)

	// If the connection is dead we should just unregister it and drop it.
	if !c.IsAlive() {
		log("%s: conn dead\n", server)
		p.unreg(server, c)
		return
	}

	// Check if there is anyone in the queue waiting for a connection to this server.
	p.queueMut.Lock()
	for e := p.queue.Front(); e != nil; e = e.Next() {
		qitem := e.Value.(*qitem)
		// Check requested servers
		for _, rserver := range qitem.servers {
			if rserver == server {
				qitem.conn = c
				p.queue.Remove(e)
				p.queueMut.Unlock()
				qitem.wakeup <- true
				return
			}
		}
	}
	p.queueMut.Unlock()

	// Just put it back in the bucket
	p.bucketsMut.Lock()
	defer p.bucketsMut.Unlock()
	bucket := p.buckets[server]
	if bucket != nil { // Strange when bucket not found
		bucket.ret(c)
	}

}
