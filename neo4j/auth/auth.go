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

type TokenManager interface {
	GetAuthToken(ctx context.Context) (auth.Token, error)
	OnTokenExpired(context.Context, auth.Token) error
}

type authTokenWithExpirationProvider = func(context.Context) (auth.Token, *time.Time, error)

type expirationBasedTokenManager struct {
	provider   authTokenWithExpirationProvider
	token      *auth.Token
	expiration *time.Time
	mutex      racing.Mutex
}

func (m *expirationBasedTokenManager) GetAuthToken(ctx context.Context) (auth.Token, error) {
	if !m.mutex.TryLock(ctx) {
		return auth.Token{}, racing.LockTimeoutError(
			"could not acquire lock in time when getting token in ExpirationBasedTokenManager")
	}
	defer m.mutex.Unlock()
	if m.token == nil || m.expiration != nil && time.Now().After(*m.expiration) {
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

// ExpirationBasedTokenManager TODO docs
func ExpirationBasedTokenManager(provider authTokenWithExpirationProvider) TokenManager {
	return &expirationBasedTokenManager{provider: provider, mutex: racing.NewMutex()}
}
