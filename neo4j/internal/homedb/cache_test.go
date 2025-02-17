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
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/auth"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
)

func TestNewCache(outer *testing.T) {
	outer.Parallel()

	outer.Run("valid maxSize", func(t *testing.T) {
		cache, err := NewCache(3)
		testutil.AssertNoError(t, err)
		testutil.AssertIntEqual(t, cache.maxSize, 3)
		testutil.AssertLen(t, cache.cache, 0)
	})

	outer.Run("invalid maxSize", func(t *testing.T) {
		_, err := NewCache(0)
		testutil.AssertError(t, err)
	})
}

func TestCache_Get(outer *testing.T) {
	outer.Parallel()

	outer.Run("get existing entry", func(t *testing.T) {
		cache, _ := NewCache(3)
		cache.SetEnabled(true)
		cache.Set("user1", "db1")
		db, exists := cache.Get("user1")
		testutil.AssertTrue(t, exists)
		testutil.AssertStringEqual(t, db, "db1")
	})

	outer.Run("get non-existent entry", func(t *testing.T) {
		cache, _ := NewCache(3)
		cache.SetEnabled(true)
		db, exists := cache.Get("user1")
		testutil.AssertFalse(t, exists)
		testutil.AssertEmptyString(t, db)
	})
}

func TestCache_Set(outer *testing.T) {
	outer.Parallel()

	outer.Run("add single entry", func(t *testing.T) {
		cache, _ := NewCache(3)
		cache.SetEnabled(true)
		cache.Set("user1", "db1")
		db, exists := cache.Get("user1")
		testutil.AssertTrue(t, exists)
		testutil.AssertStringEqual(t, db, "db1")
	})

	outer.Run("overwrite existing entry", func(t *testing.T) {
		cache, _ := NewCache(3)
		cache.SetEnabled(true)
		cache.Set("user1", "db1")
		cache.Set("user1", "db2")
		db, exists := cache.Get("user1")
		testutil.AssertTrue(t, exists)
		testutil.AssertStringEqual(t, db, "db2")
	})

	outer.Run("trigger pruning", func(t *testing.T) {
		cache, _ := NewCache(3)
		cache.SetEnabled(true)
		cache.Set("user1", "db1")
		cache.Set("user2", "db2")
		cache.Set("user3", "db3")
		cache.Set("user4", "db4") // should trigger pruning
		db, exists := cache.Get("user1")
		testutil.AssertFalse(t, exists)
		testutil.AssertEmptyString(t, db)

		db, exists = cache.Get("user2")
		testutil.AssertTrue(t, exists)
		testutil.AssertStringEqual(t, db, "db2")

		db, exists = cache.Get("user3")
		testutil.AssertTrue(t, exists)
		testutil.AssertStringEqual(t, db, "db3")

		db, exists = cache.Get("user4")
		testutil.AssertTrue(t, exists)
		testutil.AssertStringEqual(t, db, "db4")
	})
}

func TestCache_ComputeKey(outer *testing.T) {
	outer.Parallel()

	outer.Run("impersonatedUser provided", func(t *testing.T) {
		cache := &Cache{}
		key := cache.ComputeKey("impersonatedUser", nil)
		testutil.AssertStringEqual(t, key, "basic:impersonatedUser")
	})

	outer.Run("no auth or impersonatedUser provided", func(t *testing.T) {
		cache := &Cache{}
		key := cache.ComputeKey("", nil)
		testutil.AssertStringEqual(t, key, "DEFAULT")
	})

	outer.Run("auth scheme basic with principal", func(t *testing.T) {
		cache := &Cache{}
		authToken := auth.Token{
			Tokens: map[string]any{
				"scheme":    "basic",
				"principal": "userPrincipal",
			},
		}
		key := cache.ComputeKey("", &authToken)
		testutil.AssertStringEqual(t, key, "basic:userPrincipal")
	})

	outer.Run("auth scheme basic without principal", func(t *testing.T) {
		cache := &Cache{}
		authToken := auth.Token{
			Tokens: map[string]any{
				"scheme": "basic",
			},
		}
		key := cache.ComputeKey("", &authToken)
		testutil.AssertStringEqual(t, key, "basic:")
	})

	outer.Run("auth scheme kerberos", func(t *testing.T) {
		cache := &Cache{}
		authToken := auth.Token{
			Tokens: map[string]any{
				"scheme":      "kerberos",
				"credentials": "kerberosToken",
			},
		}
		key := cache.ComputeKey("", &authToken)
		testutil.AssertStringEqual(t, key, "kerberos:kerberosToken")
	})

	outer.Run("auth scheme bearer", func(t *testing.T) {
		cache := &Cache{}
		authToken := auth.Token{
			Tokens: map[string]any{
				"scheme":      "bearer",
				"credentials": "bearerToken",
			},
		}
		key := cache.ComputeKey("", &authToken)
		testutil.AssertStringEqual(t, key, "bearer:bearerToken")
	})

	outer.Run("auth scheme none", func(t *testing.T) {
		cache := &Cache{}
		authToken := auth.Token{
			Tokens: map[string]any{
				"scheme": "none",
			},
		}
		key := cache.ComputeKey("", &authToken)
		testutil.AssertStringEqual(t, key, "none")
	})

	outer.Run("auth custom scheme with parameters", func(t *testing.T) {
		cache := &Cache{}
		authToken := auth.Token{
			Tokens: map[string]any{
				"scheme": "custom",
				"parameters": map[string]any{
					"key1": "value1",
					"key2": "value2",
				},
				"credentials": "customCred",
				"realm":       "customRealm",
				"principal":   "customPrincipal",
			},
		}
		key := cache.ComputeKey("", &authToken)
		expectedKey := "scheme:custom,principal:customPrincipal,credentials:customCred,realm:customRealm,parameters:<key1>:value1;<key2>:value2;"
		testutil.AssertStringEqual(t, key, expectedKey)
	})

	outer.Run("auth custom scheme without parameters", func(t *testing.T) {
		cache := &Cache{}
		authToken := auth.Token{
			Tokens: map[string]any{
				"scheme": "custom",
			},
		}
		key := cache.ComputeKey("", &authToken)
		expectedKey := "scheme:custom,principal:<nil>,,,parameters:"
		testutil.AssertStringEqual(t, key, expectedKey)
	})

	outer.Run("auth custom scheme collision check", func(t *testing.T) {
		cache := &Cache{}
		token1 := auth.Token{
			Tokens: map[string]any{
				"scheme": "custom",
				"parameters": map[string]any{
					"key1:fun": "value1",
				},
			},
		}
		token2 := auth.Token{
			Tokens: map[string]any{
				"scheme": "custom",
				"parameters": map[string]any{
					"key1": "fun:value1",
				},
			},
		}
		key1 := cache.ComputeKey("", &token1)
		key2 := cache.ComputeKey("", &token2)
		testutil.AssertNotDeepEquals(t, key1, key2)
	})

	outer.Run("no scheme found, token is stringified", func(t *testing.T) {
		cache := &Cache{}
		authToken := auth.Token{
			Tokens: map[string]any{
				"c": "carrot",
				"a": "apple",
				"b": "banana",
			},
		}
		key := cache.ComputeKey("", &authToken)
		testutil.AssertStringEqual(t, key, "unknown:<a>:apple;<b>:banana;<c>:carrot;")
	})
}
