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
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/auth"
	"math"
	"testing"

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
	tests := []struct {
		name             string
		impersonatedUser string
		token            *auth.Token
		expectedKey      string
		wantErr          bool
	}{
		{
			name:             "impersonatedUser provided",
			impersonatedUser: "impersonatedUser",
			expectedKey:      "basic:impersonatedUser",
		},
		{
			name:        "no auth or impersonatedUser provided",
			expectedKey: "DEFAULT",
		},
		{
			name: "auth scheme basic with principal",
			token: &auth.Token{
				Tokens: map[string]any{
					"scheme":    "basic",
					"principal": "userPrincipal",
				},
			},
			expectedKey: "basic:userPrincipal",
		},
		{
			name: "auth scheme basic without principal",
			token: &auth.Token{
				Tokens: map[string]any{
					"scheme": "basic",
				},
			},
			expectedKey: "basic:",
		},
		{
			name: "auth scheme kerberos",
			token: &auth.Token{
				Tokens: map[string]any{
					"scheme":      "kerberos",
					"credentials": "kerberosToken",
				},
			},
			expectedKey: "kerberos:kerberosToken",
		},
		{
			name: "auth scheme bearer",
			token: &auth.Token{
				Tokens: map[string]any{
					"scheme":      "bearer",
					"credentials": "bearerToken",
				},
			},
			expectedKey: "bearer:bearerToken",
		},
		{
			name: "auth scheme none",
			token: &auth.Token{
				Tokens: map[string]any{
					"scheme": "none",
				},
			},
			expectedKey: "none",
		},
		{
			name: "auth custom scheme with parameters",
			token: &auth.Token{
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
			},
			expectedKey: "{\"scheme\":\"custom\",\"tokens\":{\"credentials\":\"customCred\",\"parameters\":{\"key1\":\"value1\",\"key2\":\"value2\"},\"principal\":\"customPrincipal\",\"realm\":\"customRealm\",\"scheme\":\"custom\"}}",
		},
		{
			name: "auth custom scheme without parameters",
			token: &auth.Token{
				Tokens: map[string]any{
					"scheme": "custom",
				},
			},
			expectedKey: "{\"scheme\":\"custom\",\"tokens\":{\"scheme\":\"custom\"}}",
		},
		{
			name: "no scheme found, token is stringified",
			token: &auth.Token{
				Tokens: map[string]any{
					"a": "apple",
					"b": "banana",
					"c": "carrot",
				},
			},
			expectedKey: "{\"scheme\":\"unknown\",\"tokens\":{\"a\":\"apple\",\"b\":\"banana\",\"c\":\"carrot\"}}",
		},
		{
			name: "marshal failure with non-marshallable channel",
			token: &auth.Token{
				Tokens: map[string]any{
					"scheme": "custom",
					"bad":    make(chan int),
				},
			},
			wantErr: true,
		},
		{
			name: "marshal failure with NaN",
			token: &auth.Token{
				Tokens: map[string]any{
					"scheme": "custom",
					"bad":    math.NaN(),
				},
			},
			wantErr: true,
		},
		{
			name: "marshal failure with positive infinity",
			token: &auth.Token{
				Tokens: map[string]any{
					"scheme": "custom",
					"bad":    math.Inf(1),
				},
			},
			wantErr: true,
		},
		{
			name: "marshal failure with negative infinity",
			token: &auth.Token{
				Tokens: map[string]any{
					"scheme": "custom",
					"bad":    math.Inf(-1),
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		tc := tc
		outer.Run(tc.name, func(t *testing.T) {
			cache := &Cache{}
			key, err := cache.ComputeKey(tc.impersonatedUser, tc.token)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error but got nil")
				}
				if key != "" {
					t.Errorf("expected empty key on error but got: %s", key)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if key != tc.expectedKey {
					t.Errorf("expected key %q, got %q", tc.expectedKey, key)
				}
			}
		})
	}
}
