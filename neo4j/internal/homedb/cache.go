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

package homedb

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/auth"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/errorutil"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// DefaultCacheMaxSize defines the maximum number of entries the cache can hold.
const DefaultCacheMaxSize = 1000

const keyScheme = "scheme"
const schemeNone = "none"
const schemeBasic = "basic"
const schemeKerberos = "kerberos"
const schemeBearer = "bearer"
const keyPrincipal = "principal"
const keyCredentials = "credentials"
const keyRealm = "realm"
const keyParameters = "parameters"

type cacheEntry struct {
	database string
	lastUsed time.Time
}

type Cache struct {
	maxSize     int
	pruneFactor int
	cache       map[string]*cacheEntry
	enabled     bool
	mu          sync.RWMutex
}

// NewCache creates and returns a new cache instance with the given max size.
func NewCache(maxSize int) (*Cache, error) {
	if maxSize <= 0 {
		return nil, &errorutil.UsageError{Message: "Maximum cache size must be greater than 0"}
	}

	c := 0.01
	pruneFactor := int(math.Max(1, math.Round(c*float64(maxSize)*math.Log(float64(maxSize)))))

	return &Cache{
		maxSize:     maxSize,
		pruneFactor: pruneFactor,
		cache:       make(map[string]*cacheEntry, maxSize+1),
		enabled:     false,
	}, nil
}

// Get retrieves the home database for a given user, if it exists.
// It updates the `lastUsed` timestamp for the entry.
func (c *Cache) Get(user string) (string, bool) {
	if !c.IsEnabled() {
		return "", false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.cache[user]
	if !exists {
		return "", false
	}
	entry.lastUsed = time.Now()
	return entry.database, true
}

// Set adds or updates an entry in the cache.
// If the cache exceeds its max size, it prunes the least recently used entries.
func (c *Cache) Set(user string, database string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache[user] = &cacheEntry{
		database: database,
		lastUsed: time.Now(),
	}
	c.prune()
}

// ComputeKey generates a cache key based on user impersonation and an optional session auth token.
func (c *Cache) ComputeKey(impersonatedUser string, sessionAuth *auth.Token) string {
	// If an impersonated user is provided, use it as the key.
	if impersonatedUser != "" {
		return "basic:" + impersonatedUser
	}

	// If no session authentication token is provided, return a default key.
	if sessionAuth == nil {
		return "DEFAULT"
	}

	// Process based on auth scheme
	if scheme, ok := sessionAuth.Tokens[keyScheme].(string); ok {
		switch scheme {
		case schemeBasic:
			if principal, ok := sessionAuth.Tokens[keyPrincipal].(string); ok {
				return "basic:" + principal
			}
			return "basic:"
		case schemeKerberos:
			if credentials, ok := sessionAuth.Tokens[keyCredentials].(string); ok {
				return "kerberos:" + credentials
			}
		case schemeBearer:
			if credentials, ok := sessionAuth.Tokens[keyCredentials].(string); ok {
				return "bearer:" + credentials
			}
		case schemeNone:
			return "none"
		default:
			// For custom schemes, construct the key
			var orderedParams string
			if params, ok := sessionAuth.Tokens[keyParameters].(map[string]any); ok {
				orderedParams = serializeMap(params)
			}
			var credentialString, realmString string
			if credentials, ok := sessionAuth.Tokens[keyCredentials].(string); ok && credentials != "" {
				credentialString = "credentials:" + credentials
			}
			if realm, ok := sessionAuth.Tokens[keyRealm].(string); ok && realm != "" {
				realmString = "realm:" + realm
			}
			return fmt.Sprintf("%s:%s,%s:%v,%s,%s,%s:%s",
				keyScheme, scheme,
				keyPrincipal, sessionAuth.Tokens[keyPrincipal],
				credentialString, realmString,
				keyParameters, orderedParams)
		}
	}
	// If no scheme could be found, fall back to serializing the token in a stable way.
	return fmt.Sprintf("unknown:%v", serializeMap(sessionAuth.Tokens))
}

// SetEnabled enables or disables the cache.
func (c *Cache) SetEnabled(enabled bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.enabled = enabled
}

// IsEnabled checks whether the cache is enabled.
func (c *Cache) IsEnabled() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.enabled
}

// prune removes a chunk of the least recently used entries if the cache exceeds its max size.
func (c *Cache) prune() {
	if len(c.cache) <= c.maxSize {
		return
	}

	// Collect all entries to a slice ready to be sorted.
	entries := make([]struct {
		user  string
		entry *cacheEntry
	}, 0, len(c.cache))

	for user, entry := range c.cache {
		entries = append(entries, struct {
			user  string
			entry *cacheEntry
		}{user: user, entry: entry})
	}

	// Sort the entries by `lastUsed` (oldest first).
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].entry.lastUsed.Before(entries[j].entry.lastUsed)
	})

	// Ensure we do not prune more than the number of entries in the cache
	pruneCount := c.pruneFactor
	if pruneCount > len(entries) {
		pruneCount = len(entries)
	}

	// Remove the least recently used entries
	for i := 0; i < pruneCount; i++ {
		delete(c.cache, entries[i].user)
	}
}

// serializeMap creates a deterministic string representation of a map[string]any.
func serializeMap(m map[string]any) string {
	// Extract the keys from the map.
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// Sort the keys to ensure a consistent order.
	sort.Strings(keys)

	// Build the string representation.
	var builder strings.Builder
	for _, k := range keys {
		builder.WriteString(fmt.Sprintf("<%s>:%v;", k, m[k]))
	}
	return builder.String()
}
