//go:build internal_time_mock

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

package time

import (
	"errors"
	"sync"
	"time"
)

type Ticker = func(*time.Time)

var frozenNow *time.Time = nil
var tickerRegistry []Ticker = nil
var frozenNowMutex = &sync.Mutex{}

func now() time.Time {
	if frozenNow == nil {
		return time.Now()
	}
	for _, ticker := range tickerRegistry {
		ticker(frozenNow)
	}
	return *frozenNow
}

func Now() time.Time {
	frozenNowMutex.Lock()
	defer frozenNowMutex.Unlock()

	return now()
}

func Since(t time.Time) time.Duration {
	frozenNowMutex.Lock()
	defer frozenNowMutex.Unlock()

	if frozenNow == nil {
		return time.Since(t)
	}
	return frozenNow.Sub(t)
}

func FreezeTime() error {
	frozenNowMutex.Lock()
	defer frozenNowMutex.Unlock()

	if frozenNow != nil {
		return errors.New("time already frozen")
	}
	now := now()
	frozenNow = &now
	return nil
}

func ForceFreezeTime() {
	if err := FreezeTime(); err != nil {
		panic(err)
	}
}

func TickTime(d time.Duration) error {
	frozenNowMutex.Lock()
	defer frozenNowMutex.Unlock()

	if frozenNow == nil {
		return errors.New("time not frozen")
	}
	newNow := frozenNow.Add(d)
	frozenNow = &newNow
	return nil
}

func ForceTickTime(d time.Duration) {
	if err := TickTime(d); err != nil {
		panic(err)
	}
}

func AddTicker(ticker Ticker) error {
	frozenNowMutex.Lock()
	defer frozenNowMutex.Unlock()

	if frozenNow == nil {
		return errors.New("time not frozen")
	}
	tickerRegistry = append(tickerRegistry, ticker)
	return nil
}

func ForceAddTicker(ticker Ticker) {
	if err := AddTicker(ticker); err != nil {
		panic(err)
	}
}

func UnfreezeTime() error {
	frozenNowMutex.Lock()
	defer frozenNowMutex.Unlock()

	if frozenNow == nil {
		return errors.New("time not frozen")
	}
	frozenNow = nil
	tickerRegistry = nil
	return nil
}

func ForceUnfreezeTime() {
	if err := UnfreezeTime(); err != nil {
		panic(err)
	}
}
