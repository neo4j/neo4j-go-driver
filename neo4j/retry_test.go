/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package neo4j

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRetryLogic(t *testing.T) {
	type mockResult struct {
		result interface{}
		err    error
	}

	mockWork := func(delay time.Duration, id string, r ...mockResult) func() (interface{}, string, error) {
		index := -1
		return func() (interface{}, string, error) {
			time.Sleep(delay)
			index++
			if index >= len(r) {
				index = 0
			}
			return r[index].result, id, r[index].err
		}
	}

	errorNotRetriable := fmt.Errorf("an un-retryable error")
	errorRetriable := newDatabaseError("TransientError", "Neo.TransientError.Some.Error", "transient error")

	t.Run("should return result from work if no errors", func(t *testing.T) {
		retryLogic := newRetryLogic(&Config{})

		result, err := retryLogic.retry(mockWork(0, "conn-1", mockResult{12, nil}))

		assert.Equal(t, result, 12)
		assert.NoError(t, err)
	})

	t.Run("should return error from work if error is not retriable", func(t *testing.T) {
		retryLogic := newRetryLogic(&Config{})

		result, err := retryLogic.retry(mockWork(0, "conn-1", mockResult{nil, errorNotRetriable}))

		assert.Nil(t, result)
		assert.Equal(t, err, errorNotRetriable)
	})

	t.Run("should not return result from work if error", func(t *testing.T) {
		retryLogic := newRetryLogic(&Config{})

		result, err := retryLogic.retry(mockWork(0, "conn-1", mockResult{12, errorNotRetriable}))

		assert.Nil(t, result)
		assert.Equal(t, err, errorNotRetriable)
	})

	t.Run("should retry on retriable error and return result upon successful completion", func(t *testing.T) {
		retryLogic := newRetryLogic(&Config{MaxTransactionRetryTime: 5 * time.Second})

		result, err := retryLogic.retry(mockWork(0, "conn-1", mockResult{nil, errorRetriable}, mockResult{12, nil}))

		assert.Equal(t, result, 12)
		assert.NoError(t, err)
	})

	t.Run("should return error on successive transient failures", func(t *testing.T) {
		retryLogic := newRetryLogic(&Config{MaxTransactionRetryTime: 5 * time.Second})

		result, err := retryLogic.retry(mockWork(2*time.Second, "conn-1", mockResult{nil, errorRetriable}))

		assert.Nil(t, result)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "[conn-1]: retryable operation failed to complete after")
		assert.Contains(t, err.Error(), fmt.Sprintf("[%s, %s", errorRetriable.Error(), errorRetriable.Error()))
		assert.Contains(t, err.Error(), fmt.Sprintf(", %s]", errorRetriable.Error()))
	})

}
