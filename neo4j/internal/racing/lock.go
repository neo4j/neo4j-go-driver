/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package racing

import (
	"context"
	"time"
)

type LockTimeoutError string

func (l LockTimeoutError) Error() string {
	return string(l)
}

type Mutex interface {
	// TryLock attempts to acquire the lock before the deadline set by the provided context
	// If the context does not define a deadline, TryLock will block until the lock is acquired
	// Returns true if the lock is acquired, false otherwise
	TryLock(ctx context.Context) bool
	// Unlock frees this lock, if locked
	// It is a run-time error if the lock is not locked on entry to Unlock.
	Unlock()
}

type contextLock struct {
	ch chan struct{}
}

func NewMutex() Mutex {
	return &contextLock{ch: make(chan struct{}, 1)}
}

func (c *contextLock) TryLock(ctx context.Context) bool {
	deadline, hasDeadline := ctx.Deadline()
	err := ctx.Err()
	switch {
	case !hasDeadline && err == nil:
		c.ch <- struct{}{}
		return true
	case deadline.Before(time.Now()) || err != nil:
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
	select {
	case <-c.ch:
		return
	default:
		panic("Lock is not locked")
	}

}
