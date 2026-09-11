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
	"container/list"
	"sync"
	"time"

	itime "github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/time"
)

// Default cache settings, per encryption profile.
const (
	DefaultKeyAliasCacheTTL  = 15 * time.Second
	DefaultKeyAliasCacheSize = 100
	DefaultKeyCacheTTL       = 15 * time.Minute
	DefaultKeyCacheSize      = 100
)

// Cache holds at most maxSize entries for ttl each, evicting least recently used first.
// Expiry is applied during lookups and insertions rather than by a background goroutine.
// It is safe for concurrent use.
type Cache[V any] struct {
	mutex   sync.Mutex
	ttl     time.Duration
	maxSize int
	entries map[string]*list.Element
	// order holds *cacheEntry[V], most recently used at the front.
	order *list.List
}

type cacheEntry[V any] struct {
	key     string
	value   V
	expires time.Time
}

// NewCache returns a cache holding at most maxSize entries for ttl each.
func NewCache[V any](ttl time.Duration, maxSize int) *Cache[V] {
	return &Cache[V]{
		ttl:     ttl,
		maxSize: maxSize,
		entries: make(map[string]*list.Element),
		order:   list.New(),
	}
}

// Get returns the value stored under key, if it is present and has not expired.
func (c *Cache[V]) Get(key string) (V, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	element, ok := c.entries[key]
	if !ok {
		var zero V
		return zero, false
	}
	entry := element.Value.(*cacheEntry[V])
	if c.hasExpired(entry) {
		c.remove(element)
		var zero V
		return zero, false
	}
	c.order.MoveToFront(element)
	return entry.value, true
}

// Put stores value under key, replacing any existing entry and refreshing its expiry.
func (c *Cache[V]) Put(key string, value V) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	expires := itime.Now().Add(c.ttl)
	if element, ok := c.entries[key]; ok {
		entry := element.Value.(*cacheEntry[V])
		entry.value = value
		entry.expires = expires
		c.order.MoveToFront(element)
		return
	}

	c.entries[key] = c.order.PushFront(&cacheEntry[V]{key: key, value: value, expires: expires})
	c.evict()
}

// len returns the number of entries held, including any that have expired but not yet been
// discarded.
func (c *Cache[V]) len() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.order.Len()
}

// evict brings the cache back within its size limit, discarding expired entries in
// preference to live ones.
func (c *Cache[V]) evict() {
	for c.order.Len() > c.maxSize {
		oldest := c.order.Back()
		for element := oldest; element != nil; element = element.Prev() {
			if c.hasExpired(element.Value.(*cacheEntry[V])) {
				oldest = element
				break
			}
		}
		c.remove(oldest)
	}
}

func (c *Cache[V]) hasExpired(entry *cacheEntry[V]) bool {
	return !itime.Now().Before(entry.expires)
}

func (c *Cache[V]) remove(element *list.Element) {
	delete(c.entries, element.Value.(*cacheEntry[V]).key)
	c.order.Remove(element)
}
