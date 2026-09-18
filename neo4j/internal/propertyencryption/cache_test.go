//go:build internal_neo4j_go_driver_time_mock

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

package propertyencryption

import (
	"fmt"
	"sync"
	"testing"
	"time"

	itime "github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/time"
)

// The mock clock is process wide, so these tests must not run in parallel.

// TestCacheStoresAndReturns covers lookup, replacement and misses.
func TestCacheStoresAndReturns(t *testing.T) {
	cache := NewCache[string](time.Minute, 10)

	if _, ok := cache.Get("absent"); ok {
		t.Error("an empty cache returned a value")
	}

	cache.Put("a", "first")
	got, ok := cache.Get("a")
	if !ok || got != "first" {
		t.Errorf("got %q (present: %t), want %q", got, ok, "first")
	}

	cache.Put("a", "second")
	got, _ = cache.Get("a")
	if got != "second" {
		t.Errorf("got %q after replacing, want %q", got, "second")
	}
	if cache.len() != 1 {
		t.Errorf("replacing grew the cache to %d entries", cache.len())
	}

}

// TestCacheExpiresEntries bounds how long a decapsulated key is held.
func TestCacheExpiresEntries(t *testing.T) {
	itime.ForceFreezeTime()
	defer itime.ForceUnfreezeTime()

	cache := NewCache[string](DefaultKeyCacheTTL, 10)
	cache.Put("a", "value")

	itime.ForceTickTime(DefaultKeyCacheTTL - time.Nanosecond)
	if _, ok := cache.Get("a"); !ok {
		t.Fatal("the entry expired before its ttl elapsed")
	}

	itime.ForceTickTime(time.Nanosecond)
	if _, ok := cache.Get("a"); ok {
		t.Fatal("the entry survived its ttl")
	}
	if cache.len() != 0 {
		t.Errorf("the expired entry was not discarded, %d remain", cache.len())
	}
}

// TestCacheGetDoesNotExtendTheTtl checks the ttl is measured from when an entry was stored,
// not from when it was last read.
func TestCacheGetDoesNotExtendTheTtl(t *testing.T) {
	itime.ForceFreezeTime()
	defer itime.ForceUnfreezeTime()

	cache := NewCache[string](10*time.Second, 10)
	cache.Put("a", "value")

	for i := 0; i < 9; i++ {
		itime.ForceTickTime(time.Second)
		if _, ok := cache.Get("a"); !ok {
			t.Fatalf("the entry expired after %d seconds", i+1)
		}
	}

	itime.ForceTickTime(time.Second)
	if _, ok := cache.Get("a"); ok {
		t.Fatal("reading the entry extended its ttl past 10 seconds")
	}
}

// TestCachePutRefreshesTheTtl checks storing an entry again restarts its ttl.
func TestCachePutRefreshesTheTtl(t *testing.T) {
	itime.ForceFreezeTime()
	defer itime.ForceUnfreezeTime()

	cache := NewCache[string](10*time.Second, 10)
	cache.Put("a", "value")

	itime.ForceTickTime(9 * time.Second)
	cache.Put("a", "value")

	itime.ForceTickTime(9 * time.Second)
	if _, ok := cache.Get("a"); !ok {
		t.Fatal("storing the entry again did not refresh its ttl")
	}
}

// TestCacheEvictsLeastRecentlyUsed checks eviction order once the cache is full.
func TestCacheEvictsLeastRecentlyUsed(t *testing.T) {
	itime.ForceFreezeTime()
	defer itime.ForceUnfreezeTime()

	cache := NewCache[string](time.Hour, 3)
	cache.Put("a", "1")
	cache.Put("b", "2")
	cache.Put("c", "3")

	// Reading "a" makes "b" the least recently used.
	if _, ok := cache.Get("a"); !ok {
		t.Fatal("a is missing")
	}
	cache.Put("d", "4")

	if cache.len() != 3 {
		t.Fatalf("the cache holds %d entries, want 3", cache.len())
	}
	if _, ok := cache.Get("b"); ok {
		t.Error("b survived, it was the least recently used")
	}
	for _, key := range []string{"a", "c", "d"} {
		if _, ok := cache.Get(key); !ok {
			t.Errorf("%s was evicted", key)
		}
	}
}

// TestCacheEvictsExpiredBeforeLive checks an expired entry is evicted ahead of a live one.
// The read is what makes the two orderings disagree; without it the least recently used entry
// is also the oldest and plain eviction would pick it anyway.
func TestCacheEvictsExpiredBeforeLive(t *testing.T) {
	itime.ForceFreezeTime()
	defer itime.ForceUnfreezeTime()

	cache := NewCache[string](10*time.Second, 2)
	cache.Put("stale", "1") // expires at 10s

	itime.ForceTickTime(2 * time.Second)
	cache.Put("live", "2") // expires at 12s

	// Reading "stale" makes it the more recently used, so "live" is now at the least
	// recently used end despite being the younger entry.
	if _, ok := cache.Get("stale"); !ok {
		t.Fatal("stale is missing")
	}

	itime.ForceTickTime(9 * time.Second) // now 11s: "stale" has expired, "live" has not
	cache.Put("new", "3")

	if cache.len() != 2 {
		t.Fatalf("the cache holds %d entries, want 2", cache.len())
	}
	if _, ok := cache.Get("stale"); ok {
		t.Error("the expired entry survived")
	}
	if _, ok := cache.Get("live"); !ok {
		t.Error("the live entry was evicted in favour of an expired one")
	}
}

// TestCacheNeverExceedsItsSize checks the size bound holds across many insertions.
func TestCacheNeverExceedsItsSize(t *testing.T) {
	cache := NewCache[int](time.Hour, 5)
	for i := 0; i < 100; i++ {
		cache.Put(fmt.Sprintf("key-%d", i), i)
		if cache.len() > 5 {
			t.Fatalf("the cache grew to %d entries after %d insertions", cache.len(), i+1)
		}
	}
}

// TestCacheIsSafeForConcurrentUse checks concurrent access under the race detector.
func TestCacheIsSafeForConcurrentUse(t *testing.T) {
	cache := NewCache[int](time.Hour, 16)

	var waitGroup sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		waitGroup.Add(1)
		go func(worker int) {
			defer waitGroup.Done()
			for i := 0; i < 500; i++ {
				key := fmt.Sprintf("key-%d", i%32)
				cache.Put(key, i)
				cache.Get(key)
				cache.len()
			}
		}(worker)
	}
	waitGroup.Wait()

	if cache.len() > 16 {
		t.Errorf("the cache holds %d entries, want at most 16", cache.len())
	}
}
