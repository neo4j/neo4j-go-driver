package racing_test

import (
	"context"
	"github.com/DaChartreux/neo4j-go-driver/v5/neo4j/internal/racing"
	"github.com/DaChartreux/neo4j-go-driver/v5/neo4j/internal/testutil"
	"testing"
	"time"
)

func TestMutex(outer *testing.T) {
	outer.Parallel()

	backgroundCtx := context.Background()

	outer.Run("panics when unlocking unlocked lock", func(t *testing.T) {
		mutex := racing.NewMutex()

		testutil.AssertPanics(t, mutex.Unlock)
	})

	outer.Run("locks and unlocks successfully without deadline", func(t *testing.T) {
		mutex := racing.NewMutex()

		result := mutex.TryLock(backgroundCtx)
		mutex.Unlock()

		testutil.AssertTrue(t, result)
	})

	outer.Run("locks and unlocks successfully without deadline after first unlocking", func(t *testing.T) {
		mutex := racing.NewMutex()
		result := make(chan bool, 1)

		mutex.TryLock(backgroundCtx)
		go func() {
			result <- mutex.TryLock(backgroundCtx)
		}()
		mutex.Unlock()

		testutil.AssertTrue(t, <-result)
		mutex.Unlock()
	})

	outer.Run("fails to lock and panics when unlocking with a canceled context", func(t *testing.T) {
		mutex := racing.NewMutex()

		result := mutex.TryLock(canceledContext())

		testutil.AssertFalse(t, result)
		testutil.AssertPanics(t, mutex.Unlock)
	})

	outer.Run("fails to lock and panics when unlocking after deadline reached", func(t *testing.T) {
		delay := 20 * time.Millisecond
		timeout, cancelFunc := context.WithTimeout(backgroundCtx, delay)
		defer cancelFunc()
		mutex := racing.NewMutex()
		time.Sleep(2 * delay)

		result := mutex.TryLock(timeout)

		testutil.AssertFalse(t, result)
		testutil.AssertPanics(t, mutex.Unlock)
	})

	outer.Run("fails to lock and panics when unlocking after another routine unlocks the lock after deadline", func(t *testing.T) {
		mutex := racing.NewMutex()
		result := make(chan bool, 1)
		delay := 20 * time.Millisecond

		mutex.TryLock(backgroundCtx)
		go func() {
			timeout, cancelFunc := context.WithTimeout(backgroundCtx, delay)
			defer cancelFunc()
			result <- mutex.TryLock(timeout)
		}()
		time.Sleep(2 * delay)

		mutex.Unlock()
		testutil.AssertFalse(t, <-result)
		testutil.AssertPanics(t, mutex.Unlock)
	})
}
