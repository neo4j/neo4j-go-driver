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
	"math"
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
		key, _ := cache.ComputeKey("impersonatedUser", nil)
		testutil.AssertStringEqual(t, key, "basic:impersonatedUser")
	})

	outer.Run("no auth or impersonatedUser provided", func(t *testing.T) {
		cache := &Cache{}
		key, _ := cache.ComputeKey("", nil)
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
		key, _ := cache.ComputeKey("", &authToken)
		testutil.AssertStringEqual(t, key, "basic:userPrincipal")
	})

	outer.Run("auth scheme basic without principal", func(t *testing.T) {
		cache := &Cache{}
		authToken := auth.Token{
			Tokens: map[string]any{
				"scheme": "basic",
			},
		}
		key, _ := cache.ComputeKey("", &authToken)
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
		key, _ := cache.ComputeKey("", &authToken)
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
		key, _ := cache.ComputeKey("", &authToken)
		testutil.AssertStringEqual(t, key, "bearer:bearerToken")
	})

	outer.Run("auth scheme none", func(t *testing.T) {
		cache := &Cache{}
		authToken := auth.Token{
			Tokens: map[string]any{
				"scheme": "none",
			},
		}
		key, _ := cache.ComputeKey("", &authToken)
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
		key, _ := cache.ComputeKey("", &authToken)
		expectedKey := "{\"scheme\":\"custom\",\"tokens\":{\"credentials\":\"customCred\",\"parameters\":{\"key1\":\"value1\",\"key2\":\"value2\"},\"principal\":\"customPrincipal\",\"realm\":\"customRealm\",\"scheme\":\"custom\"}}"
		testutil.AssertStringEqual(t, key, expectedKey)
	})

	outer.Run("auth custom scheme without parameters", func(t *testing.T) {
		cache := &Cache{}
		authToken := auth.Token{
			Tokens: map[string]any{
				"scheme": "custom",
			},
		}
		key, _ := cache.ComputeKey("", &authToken)
		expectedKey := "{\"scheme\":\"custom\",\"tokens\":{\"scheme\":\"custom\"}}"
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
		key1, _ := cache.ComputeKey("", &token1)
		key2, _ := cache.ComputeKey("", &token2)
		testutil.AssertNotDeepEquals(t, key1, key2)
	})

	outer.Run("auth custom scheme collision check 2", func(t *testing.T) {
		cache := &Cache{}
		token1 := auth.Token{
			Tokens: map[string]any{
				"scheme": "funky,principal:banana",
			},
		}
		token2 := auth.Token{
			Tokens: map[string]any{
				"scheme":    "funky",
				"principal": "banana,principal:<nil>",
			},
		}
		key1, _ := cache.ComputeKey("", &token1)
		key2, _ := cache.ComputeKey("", &token2)
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
		key, _ := cache.ComputeKey("", &authToken)
		testutil.AssertStringEqual(t, key, "{\"scheme\":\"unknown\",\"tokens\":{\"a\":\"apple\",\"b\":\"banana\",\"c\":\"carrot\"}}")
	})

	outer.Run("marshal failure", func(t *testing.T) {
		cache := &Cache{}
		authToken := auth.Token{
			Tokens: map[string]any{
				"scheme": "custom",
				"bad":    make(chan int), // non-marshallable value
			},
		}
		key, err := cache.ComputeKey("", &authToken)
		testutil.AssertError(t, err)
		testutil.AssertEmptyString(t, key)
	})
}

func TestCache_ComputeKey_Collisions(outer *testing.T) {
	outer.Parallel()

	outer.Run("float special values: +0 vs -0", func(t *testing.T) {
		cache := &Cache{}
		tokenPos0 := auth.Token{
			Tokens: map[string]any{
				"scheme": "custom",
				"val":    +0.0,
			},
		}
		tokenNeg0 := auth.Token{
			Tokens: map[string]any{
				"scheme": "custom",
				"val":    -0.0,
			},
		}
		keyPos, _ := cache.ComputeKey("", &tokenPos0)
		keyNeg, _ := cache.ComputeKey("", &tokenNeg0)
		testutil.AssertNotDeepEquals(t, keyPos, keyNeg)
	})

	outer.Run("float special values: +Inf vs -Inf", func(t *testing.T) {
		cache := &Cache{}
		tokenInf := auth.Token{
			Tokens: map[string]any{
				"scheme": "custom",
				"val":    math.Inf(1),
			},
		}
		tokenNegInf := auth.Token{
			Tokens: map[string]any{
				"scheme": "custom",
				"val":    math.Inf(-1),
			},
		}
		keyInf, _ := cache.ComputeKey("", &tokenInf)
		keyNegInf, _ := cache.ComputeKey("", &tokenNegInf)
		testutil.AssertNotDeepEquals(t, keyInf, keyNegInf)
	})

	outer.Run("float special values: NaN vs NaN", func(t *testing.T) {
		cache := &Cache{}
		token1 := auth.Token{
			Tokens: map[string]any{
				"scheme": "custom",
				"val":    math.NaN(),
			},
		}
		token2 := auth.Token{
			Tokens: map[string]any{
				"scheme": "custom",
				"val":    math.NaN(),
			},
		}
		key1, _ := cache.ComputeKey("", &token1)
		key2, _ := cache.ComputeKey("", &token2)
		testutil.AssertDeepEquals(t, key1, key2)
	})

	outer.Run("different map types produce equivalent keys", func(t *testing.T) {
		cache := &Cache{}
		token1 := auth.Token{
			Tokens: map[string]any{
				"scheme": "custom",
				"params": map[string]any{
					"key": "value",
				},
			},
		}
		token2 := auth.Token{
			Tokens: map[string]any{
				"scheme": "custom",
				"params": map[string]string{
					"key": "value",
				},
			},
		}
		key1, _ := cache.ComputeKey("", &token1)
		key2, _ := cache.ComputeKey("", &token2)
		testutil.AssertDeepEquals(t, key1, key2)
	})

	outer.Run("new types are distinguished from their underlying types", func(t *testing.T) {
		type MyInt int64
		cache := &Cache{}
		tokenMyInt := auth.Token{
			Tokens: map[string]any{
				"scheme": "custom",
				"val":    MyInt(42),
			},
		}
		tokenInt := auth.Token{
			Tokens: map[string]any{
				"scheme": "custom",
				"val":    int64(42),
			},
		}
		keyMyInt, _ := cache.ComputeKey("", &tokenMyInt)
		keyInt, _ := cache.ComputeKey("", &tokenInt)
		testutil.AssertNotDeepEquals(t, keyMyInt, keyInt)
	})

	outer.Run("byte arrays collide with equivalent strings", func(t *testing.T) {
		cache := &Cache{}
		byteData := []uint64{1, 2, 3, 5}
		encodedString := "AQIDBQ=="
		tokenBytes := auth.Token{
			Tokens: map[string]any{
				"scheme": "custom",
				"data":   byteData,
			},
		}
		tokenString := auth.Token{
			Tokens: map[string]any{
				"scheme": "custom",
				"data":   encodedString,
			},
		}
		keyBytes, _ := cache.ComputeKey("", &tokenBytes)
		keyString, _ := cache.ComputeKey("", &tokenString)
		testutil.AssertDeepEquals(t, keyBytes, keyString)
	})

	outer.Run("complex nested structures yield equivalent keys", func(t *testing.T) {
		type MyInt int64
		cache := &Cache{}
		token1 := auth.Token{
			Tokens: map[string]any{
				"scheme": "custom",
				"nested": map[string]any{
					"a": []any{
						MyInt(1),
						1.0,
						[]uint8{10, 20, 30},
						map[int]string{
							1: "one",
							2: "two",
						},
					},
				},
			},
		}
		token2 := auth.Token{
			Tokens: map[string]any{
				"scheme": "custom",
				"nested": map[string]any{
					"a": []any{
						1.0,
						MyInt(1),
						[]uint8{10, 20, 30},
						map[int]string{
							2: "two",
							1: "one",
						},
					},
				},
			},
		}
		key1, _ := cache.ComputeKey("", &token1)
		key2, _ := cache.ComputeKey("", &token2)
		testutil.AssertDeepEquals(t, key1, key2)
	})

	outer.Run("TODO-example", func(t *testing.T) {
		type MyType int64
		ch := make(chan int)
		cache := &Cache{}
		token1 := auth.Token{
			Tokens: map[string]any{
				"scheme":     "custom",
				"signedZero": +0.0,
				"signedInf":  math.Inf(1),
				"nan":        math.NaN(),
				"float":      1.0,
				"int":        1,
				"MyType":     MyType(1),
				"byteIssue":  []uint64{1, 2, 3, 5},
				"nestedMap": map[string]any{
					"a": "apple",
					"b": "banana",
				},
				"chan": ch,
			},
		}
		token2 := auth.Token{
			Tokens: map[string]any{
				"scheme":     "custom",
				"signedZero": -0.0,
				"signedInf":  math.Inf(-1),
				"nan":        math.NaN(),
				"float":      1.0,
				"int":        1,
				"MyType":     MyType(1),
				"byteIssue":  "AQIDBQ==",
				"nestedMap": map[string]string{
					"b": "banana",
					"a": "apple",
				},
				"chan": ch,
			},
		}
		key1, _ := cache.ComputeKey("", &token1)
		key2, _ := cache.ComputeKey("", &token2)
		fmt.Println(key1)
		fmt.Println(key2)
		testutil.AssertDeepEquals(t, key1, key2)
	})
}
