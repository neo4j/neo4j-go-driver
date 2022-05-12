package racingio_test

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/racingio"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	"sync"
	"testing"
	"time"
)

func TestMutex(outer *testing.T) {
	backgroundCtx := context.Background()

	outer.Run("locks successfully without deadline", func(t *testing.T) {
		mutex := racingio.NewMutex()

		result := mutex.TryLock(backgroundCtx)

		testutil.AssertTrue(t, result)
	})

	outer.Run("locks successfully without deadline after first unlocking", func(t *testing.T) {
		lockedSecondTime := false
		mutex := racingio.NewMutex()
		var wait sync.WaitGroup
		wait.Add(1)

		mutex.TryLock(backgroundCtx)
		go func() {
			defer wait.Done()
			lockedSecondTime = mutex.TryLock(backgroundCtx)
		}()
		mutex.Unlock()
		wait.Wait()

		testutil.AssertTrue(t, lockedSecondTime)
	})

	outer.Run("fails to lock after deadline reached", func(t *testing.T) {
		delay := 20 * time.Millisecond
		timeout, cancelFunc := context.WithTimeout(backgroundCtx, delay)
		defer cancelFunc()
		mutex := racingio.NewMutex()
		time.Sleep(2 * delay)

		result := mutex.TryLock(timeout)

		testutil.AssertFalse(t, result)
	})
}
