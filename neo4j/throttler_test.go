/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
	"testing"
	"time"
)

func TestThrottler(t *testing.T) {
	assertDurationVary := func(d1, d2 time.Duration) {
		t.Logf("%s -> %s", d1, d2)
		if d2 == d1 {
			t.Errorf("Delay should vary, %s -> %s", d1, d2)
		}
	}

	thr := throttler(1 * time.Second)
	d1 := thr.delay()
	var d2 time.Duration
	for i := 0; i < 10; i++ {
		thr = thr.next()
		d2 = thr.delay()
		assertDurationVary(d1, d2)
		d1 = d2
	}
}
