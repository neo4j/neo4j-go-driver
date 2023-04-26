/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
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

package auth

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/auth"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/racing"
	"reflect"
	"time"
)

// TokenManager is an interface for components that can provide auth tokens.
// The `neo4j` package provides default implementations of `auth.TokenManager` for common authentication schemes.
// See `neo4j.NewDriverWithContext`.
// Custom implementations of this class can be used to provide more complex authentication refresh functionality.
//
// WARNING:
//
//	The manager *must not* interact with the driver in any way as this can cause deadlocks and undefined behaviour.
//	Furthermore, the manager is expected to be thread-safe.
//
// TokenManager is part of the re-authentication preview feature
// (see README on what it means in terms of support and compatibility guarantees)
type TokenManager interface {
	// GetAuthToken retrieves an auth.Token or returns an error if the retrieval fails.
	// auth.Token can be created with built-in functions such as:
	//   - `neo4j.NoAuth`
	//   - `neo4j.BasicAuth`
	//   - `neo4j.KerberosAuth`
	//   - `neo4j.BearerAuth`
	//   - `neo4j.CustomAuth`
	//
	// The token returned must always belong to the same identity.
	// Switching identities using the `TokenManager` is undefined behavior.
	GetAuthToken(ctx context.Context) (auth.Token, error)
	// OnTokenExpired is called by the driver when the provided token expires
	// OnTokenExpired should invalidate the current token if it matches the provided one
	OnTokenExpired(context.Context, auth.Token) error
}

type authTokenWithExpirationProvider = func(context.Context) (auth.Token, *time.Time, error)

type expirationBasedTokenManager struct {
	provider   authTokenWithExpirationProvider
	token      *auth.Token
	expiration *time.Time
	mutex      racing.Mutex
	now        *func() time.Time
}

func (m *expirationBasedTokenManager) GetAuthToken(ctx context.Context) (auth.Token, error) {
	if !m.mutex.TryLock(ctx) {
		return auth.Token{}, racing.LockTimeoutError(
			"could not acquire lock in time when getting token in ExpirationBasedTokenManager")
	}
	defer m.mutex.Unlock()
	if m.token == nil || m.expiration != nil && (*m.now)().After(*m.expiration) {
		token, expiration, err := m.provider(ctx)
		if err != nil {
			return auth.Token{}, err
		}
		m.token = &token
		m.expiration = expiration
	}
	return *m.token, nil
}

func (m *expirationBasedTokenManager) OnTokenExpired(ctx context.Context, token auth.Token) error {
	if !m.mutex.TryLock(ctx) {
		return racing.LockTimeoutError(
			"could not acquire lock in time when handling token expiration in ExpirationBasedTokenManager")
	}
	defer m.mutex.Unlock()
	if m.token != nil && reflect.DeepEqual(token.Tokens, m.token.Tokens) {
		m.token = nil
	}
	return nil
}

// ExpirationBasedTokenManager creates a token manager for potentially expiring auth info.
//
// The first and only argument is a provider function that returns auth information and an optional expiration time.
// If the expiration time is nil, the auth info is assumed to never expire.
//
// WARNING:
//
//	The provider function *must not* interact with the driver in any way as this can cause deadlocks and undefined
//	behaviour.
//
//	The provider function only ever return auth information belonging to the same identity.
//	Switching identities is undefined behavior.
//
// ExpirationBasedTokenManager is part of the re-authentication preview feature
// (see README on what it means in terms of support and compatibility guarantees)
func ExpirationBasedTokenManager(provider authTokenWithExpirationProvider) TokenManager {
	now := time.Now
	return &expirationBasedTokenManager{provider: provider, mutex: racing.NewMutex(), now: &now}
}
