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

package neo4j

import (
	"testing"

	. "github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/testutil"
)

func TestAuthTokens(t *testing.T) {
	parameters := map[string]any{
		"user_id":     "1234",
		"user_emails": []string{"a@b.com", "b@c.com"},
	}

	testCases := []struct {
		name     string
		token    AuthToken
		expected map[string]any
	}{
		{
			name:     "no auth",
			token:    NoAuth(),
			expected: map[string]any{"scheme": "none"},
		},
		{
			name:  "basic",
			token: BasicAuth("user", "password", ""),
			expected: map[string]any{
				"scheme":      "basic",
				"principal":   "user",
				"credentials": "password",
			},
		},
		{
			name:  "basic with realm",
			token: BasicAuth("user", "password", "realm"),
			expected: map[string]any{
				"scheme":      "basic",
				"principal":   "user",
				"credentials": "password",
				"realm":       "realm",
			},
		},
		{
			name:  "kerberos",
			token: KerberosAuth("ticket"),
			expected: map[string]any{
				"scheme":      "kerberos",
				"principal":   "",
				"credentials": "ticket",
			},
		},
		{
			name:  "bearer",
			token: BearerAuth("token"),
			expected: map[string]any{
				"scheme":      "bearer",
				"credentials": "token",
			},
		},
		{
			name:  "bearer with realm",
			token: BearerAuthWithRealm("token", "realm"),
			expected: map[string]any{
				"scheme":      "bearer",
				"credentials": "token",
				"realm":       "realm",
			},
		},
		{
			name:  "custom with nil parameters",
			token: CustomAuth("custom_scheme", "user", "password", "realm", nil),
			expected: map[string]any{
				"scheme":      "custom_scheme",
				"principal":   "user",
				"credentials": "password",
				"realm":       "realm",
			},
		},
		{
			name:  "custom without password, realm or parameters",
			token: CustomAuth("custom_scheme", "user", "", "", nil),
			expected: map[string]any{
				"scheme":    "custom_scheme",
				"principal": "user",
			},
		},
		{
			name:  "custom with empty parameters",
			token: CustomAuth("custom_scheme", "user", "password", "realm", map[string]any{}),
			expected: map[string]any{
				"scheme":      "custom_scheme",
				"principal":   "user",
				"credentials": "password",
				"realm":       "realm",
			},
		},
		{
			name:  "custom with parameters",
			token: CustomAuth("custom_scheme", "user", "password", "realm", parameters),
			expected: map[string]any{
				"scheme":      "custom_scheme",
				"principal":   "user",
				"credentials": "password",
				"realm":       "realm",
				"parameters":  parameters,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			AssertDeepEquals(t, testCase.expected, testCase.token.Tokens)
		})
	}
}
