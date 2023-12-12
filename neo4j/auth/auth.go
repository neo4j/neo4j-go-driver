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

package auth

import (
	"context"
	"reflect"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/auth"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/collections"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/racing"
	itime "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/time"
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

	// HandleSecurityException is called when the server returns any `Neo.ClientError.Security.*` error.
	// It should return true if the error was handled, in which case the driver will mark the error as retryable.
	HandleSecurityException(context.Context, auth.Token, *db.Neo4jError) (bool, error)
}

type authTokenProvider = func(context.Context) (auth.Token, error)

type authTokenWithExpirationProvider = func(context.Context) (auth.Token, *time.Time, error)

type neo4jAuthTokenManager struct {
	provider             authTokenWithExpirationProvider
	token                *auth.Token
	expiration           *time.Time
	mutex                racing.Mutex
	handledSecurityCodes collections.Set[string]
}

func (m *neo4jAuthTokenManager) GetAuthToken(ctx context.Context) (auth.Token, error) {
	if !m.mutex.TryLock(ctx) {
		return auth.Token{}, racing.LockTimeoutError(
			"could not acquire lock in time when getting token in neo4jAuthTokenManager")
	}
	defer m.mutex.Unlock()
	if m.token == nil || m.expiration != nil && itime.Now().After(*m.expiration) {
		token, expiration, err := m.provider(ctx)
		if err != nil {
			return auth.Token{}, err
		}
		m.token = &token
		m.expiration = expiration
	}
	return *m.token, nil
}

func (m *neo4jAuthTokenManager) HandleSecurityException(ctx context.Context, token auth.Token, securityException *db.Neo4jError) (bool, error) {
	if !m.handledSecurityCodes.Contains(securityException.Code) {
		return false, nil
	}
	if !m.mutex.TryLock(ctx) {
		return false, racing.LockTimeoutError(
			"could not acquire lock in time when handling security exception in neo4jAuthTokenManager")
	}
	defer m.mutex.Unlock()
	if m.token != nil && reflect.DeepEqual(token.Tokens, m.token.Tokens) {
		m.token = nil
	}
	return true, nil
}

// BasicTokenManager generates a TokenManager to manage basic auth password rotation.
// The provider is invoked solely when a new token instance is required, triggered by server
// rejection of the current token due to an authentication exception.
//
// WARNING:
//
// The provider function *must not* interact with the driver in any way as this can cause deadlocks and undefined
// behaviour.
//
// The provider function must only ever return auth information belonging to the same identity.
// Switching identities is undefined behavior.
func BasicTokenManager(provider authTokenProvider) TokenManager {
	return &neo4jAuthTokenManager{
		provider: wrapWithNilExpiration(provider),
		mutex:    racing.NewMutex(),
		handledSecurityCodes: collections.NewSet([]string{
			"Neo.ClientError.Security.Unauthorized",
		}),
	}
}

// BearerTokenManager generates a TokenManager to manage possibly expiring authentication details.
//
// The provider is invoked when a new token instance is required, triggered by server
// rejection of the current token due to authentication or token expiration exceptions.
//
// WARNING:
//
// The provider function *must not* interact with the driver in any way as this can cause deadlocks and undefined
// behaviour.
//
// The provider function must only ever return auth information belonging to the same identity.
// Switching identities is undefined behavior.
func BearerTokenManager(provider authTokenWithExpirationProvider) TokenManager {
	return &neo4jAuthTokenManager{
		provider: provider,
		mutex:    racing.NewMutex(),
		handledSecurityCodes: collections.NewSet([]string{
			"Neo.ClientError.Security.TokenExpired",
			"Neo.ClientError.Security.Unauthorized",
		}),
	}
}

func wrapWithNilExpiration(provider authTokenProvider) authTokenWithExpirationProvider {
	return func(ctx context.Context) (auth.Token, *time.Time, error) {
		token, err := provider(ctx)
		return token, nil, err
	}
}
