package racing

import (
	"context"
	"time"
)

type Mutex interface {
	// TryLock attempts to acquire the lock before the deadline set by the provided context
	// If the context does not define a deadline, TryLock will block until the lock is acquired
	// Returns true if the lock is acquired in time, false otherwise
	TryLock(ctx context.Context) bool
	// Unlock frees this lock. This should only be called if TryLock is successful.
	Unlock()
}

type contextLock struct {
	ch chan struct{}
}

func NewMutex() Mutex {
	return &contextLock{ch: make(chan struct{}, 1)}
}

func (c *contextLock) TryLock(ctx context.Context) bool {
	if deadline, hasDeadline := ctx.Deadline(); !hasDeadline {
		c.ch <- struct{}{}
		return true
	} else if deadline.Before(time.Now()) {
		// if the channel is already done and the lock is free
		// select will pick any of the case at random
		return false
	}
	select {
	case c.ch <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (c *contextLock) Unlock() {
	<-c.ch
}
