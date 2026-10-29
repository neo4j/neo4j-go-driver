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

func TestCacheEvictsLeastRecentlyUsed(t *testing.T) {
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

func TestCachePutPurgesExpiredEntries(t *testing.T) {
	itime.ForceFreezeTime()
	defer itime.ForceUnfreezeTime()

	cache := NewCache[string](10*time.Second, 10)
	cache.Put("stale", "1")
	cache.Put("staler", "2")
	itime.ForceTickTime(5 * time.Second)
	cache.Put("live", "3")
	itime.ForceTickTime(6 * time.Second)
	cache.Put("new", "4")

	if cache.len() != 2 {
		t.Fatalf("the cache holds %d entries, want 2", cache.len())
	}
	for _, key := range []string{"live", "new"} {
		if _, ok := cache.Get(key); !ok {
			t.Errorf("%s was purged before its ttl", key)
		}
	}
}

func TestCacheIsSafeForConcurrentUse(t *testing.T) {
	cache := NewCache[int](time.Hour, 16)

	var waitGroup sync.WaitGroup
	for range 8 {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			for i := 0; i < 500; i++ {
				key := fmt.Sprintf("key-%d", i%32)
				cache.Put(key, i)
				cache.Get(key)
			}
		}()
	}
	waitGroup.Wait()
}
