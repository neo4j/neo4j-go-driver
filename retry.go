/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

package neo4j_go_driver

import (
    "time"
    "math/rand"
)

type retryLogic struct {
    logging           Logging
    initialRetryDelay time.Duration
    maxRetryTime      time.Duration
    delayMultiplier   float64
    delayJitter       float64
}

func newRetryLogic(config *Config) *retryLogic {
    return &retryLogic{
        logging:           config.log,
        initialRetryDelay: 1 * time.Second,
        maxRetryTime:      config.maxTransactionRetryDuration,
        delayMultiplier:   2.0,
        delayJitter:       0.2,
    }
}

func (logic *retryLogic) computeDelayWithJitter(delay time.Duration) time.Duration {
    jitter := time.Duration(float64(delay) * logic.delayJitter)
    nextDelay := delay - jitter + time.Duration(2*float64(jitter)*rand.Float64())
    return nextDelay
}

func (logic *retryLogic) retry(work func() (interface{}, error)) (interface{}, error) {
    result := interface{}(nil)
    err := error(nil)
    startTime := time.Now()
    nextDelay := logic.initialRetryDelay

    for true {
        result, err = work()
        if err == nil {
            return result, nil
        }

        if isRetriableError(err) {
            elapsed := time.Since(startTime)
            if elapsed < logic.maxRetryTime {
                delayWithJitter := logic.computeDelayWithJitter(nextDelay)
                warningf(logic.logging, "retriable operation failed to complete [error: %s] and will be retried in %dms", err.Error(), delayWithJitter.Nanoseconds()/int64(time.Millisecond))
                time.Sleep(delayWithJitter)
                nextDelay = delayWithJitter
                // TODO: record error
                continue
            }
        }

        break
    }

    return nil, err
}
