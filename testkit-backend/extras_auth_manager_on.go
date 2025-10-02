//go:build !internal_neo4j_testkit_no_auth_manager

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

package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/auth"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
)

const extrasAuthManager = "authManager"

func init() {
	registerExtra(
		extrasAuthManager,
		ExtrasRegisterEntry{
			newBackendExtraData: newExtrasAuthManagerExtraData,
			extraRequestHandlers: map[string]extrasRequestHandlerFunc{
				"NewAuthTokenManager":                              newAuthTokenManagerHandler,
				"AuthTokenManagerGetAuthCompleted":                 authTokenManagerGetAuthCompletedHandler,
				"AuthTokenManagerHandleSecurityExceptionCompleted": authTokenManagerHandleSecurityExceptionCompletedHandler,
				"NewBasicAuthTokenManager":                         newBasicAuthTokenManagerHandler,
				"BasicAuthTokenProviderCompleted":                  basicAuthTokenProviderCompletedHandler,
				"NewBearerAuthTokenManager":                        newBearerAuthTokenManagerHandler,
				"BearerAuthTokenProviderCompleted":                 bearerAuthTokenProviderCompletedHandler,
				"AuthTokenManagerClose":                            authTokenManagerCloseHandler,
			},
		},
	)
}

type extrasAuthManagerExtraData struct {
	authTokenManagers               map[string]auth.TokenManager
	resolvedGetAuthTokens           map[string]neo4j.AuthToken
	resolvedHandleSecurityException map[string]bool
	resolvedBasicTokens             map[string]AuthToken
	resolvedBearerTokens            map[string]AuthTokenAndExpiration
}

func newExtrasAuthManagerExtraData() any {
	return extrasAuthManagerExtraData{
		authTokenManagers:               make(map[string]auth.TokenManager),
		resolvedGetAuthTokens:           make(map[string]neo4j.AuthToken),
		resolvedHandleSecurityException: make(map[string]bool),
		resolvedBasicTokens:             make(map[string]AuthToken),
		resolvedBearerTokens:            make(map[string]AuthTokenAndExpiration),
	}
}

func extrasAuthManagerGetBackendExtraData(backend *backend) extrasAuthManagerExtraData {
	return getBackendExtraData(backend, extrasAuthManager).(extrasAuthManagerExtraData)
}

func getDriverAuthToken(backend *backend, data map[string]any) (authToken auth.TokenManager, err error) {
	rawAuth := data["authorizationToken"]
	if rawAuth == nil {
		managerId := data["authTokenManagerId"].(string)
		authToken = extrasAuthManagerGetBackendExtraData(backend).authTokenManagers[managerId]
	} else {
		authToken, err = getAuth(rawAuth.(map[string]any)["data"].(map[string]any))
	}
	return
}

type GenericTokenManager struct {
	GetAuthTokenFunc            func() neo4j.AuthToken
	HandleSecurityExceptionFunc func(neo4j.AuthToken, *db.Neo4jError) bool
}

type AuthToken struct {
	token neo4j.AuthToken
}

type AuthTokenAndExpiration struct {
	token      neo4j.AuthToken
	expiration *time.Time
}

func (g GenericTokenManager) GetAuthToken(_ context.Context) (neo4j.AuthToken, error) {
	return g.GetAuthTokenFunc(), nil
}

func (g GenericTokenManager) HandleSecurityException(_ context.Context, token neo4j.AuthToken, securityException *db.Neo4jError) (bool, error) {
	handled := g.HandleSecurityExceptionFunc(token, securityException)
	return handled, nil
}

func newAuthTokenManagerHandler(backend *backend, data map[string]any) {
	backendData := extrasAuthManagerGetBackendExtraData(backend)
	managerId := backend.nextId()

	manager := GenericTokenManager{
		GetAuthTokenFunc: func() neo4j.AuthToken {
			id := backend.nextId()
			backend.writeResponse(
				"AuthTokenManagerGetAuthRequest",
				map[string]any{
					"id":                 id,
					"authTokenManagerId": managerId,
				})
			for backend.process() {
				if token, ok := backendData.resolvedGetAuthTokens[id]; ok {
					delete(backendData.resolvedGetAuthTokens, id)
					return token
				}
			}
			return neo4j.AuthToken{}
		},
		HandleSecurityExceptionFunc: func(token neo4j.AuthToken, error *db.Neo4jError) bool {
			id := backend.nextId()
			backend.writeResponse(
				"AuthTokenManagerHandleSecurityExceptionRequest",
				map[string]any{
					"id":                 id,
					"authTokenManagerId": managerId,
					"auth":               serializeAuth(token),
					"errorCode":          error.Code,
				})
			for backend.process() {
				if handled, ok := backendData.resolvedHandleSecurityException[id]; ok {
					delete(backendData.resolvedHandleSecurityException, id)
					return handled
				}
			}
			return false
		},
	}
	backendData.authTokenManagers[managerId] = manager
	backend.writeResponse("AuthTokenManager", map[string]any{"id": managerId})
}

func authTokenManagerGetAuthCompletedHandler(backend *backend, data map[string]any) {
	backendData := extrasAuthManagerGetBackendExtraData(backend)

	id := data["requestId"].(string)
	token, err := getAuth(data["auth"].(map[string]any)["data"].(map[string]any))
	if err != nil {
		backend.writeError(err)
		return
	}
	backendData.resolvedGetAuthTokens[id] = token
}

func authTokenManagerHandleSecurityExceptionCompletedHandler(backend *backend, data map[string]any) {
	backendData := extrasAuthManagerGetBackendExtraData(backend)

	handled := data["handled"].(bool)
	id := data["requestId"].(string)
	backendData.resolvedHandleSecurityException[id] = handled
}

func newBasicAuthTokenManagerHandler(backend *backend, data map[string]any) {
	backendData := extrasAuthManagerGetBackendExtraData(backend)
	managerId := backend.nextId()

	manager := auth.BasicTokenManager(
		func(context.Context) (neo4j.AuthToken, error) {
			id := backend.nextId()
			backend.writeResponse(
				"BasicAuthTokenProviderRequest",
				map[string]any{
					"id":                      id,
					"basicAuthTokenManagerId": managerId,
				})
			for backend.process() {
				if basicToken, ok := backendData.resolvedBasicTokens[id]; ok {
					delete(backendData.resolvedBasicTokens, id)
					return basicToken.token, nil
				}
			}
			return neo4j.AuthToken{}, nil
		})
	backendData.authTokenManagers[managerId] = manager
	backend.writeResponse("BasicAuthTokenManager", map[string]any{"id": managerId})
}

func basicAuthTokenProviderCompletedHandler(backend *backend, data map[string]any) {
	backendData := extrasAuthManagerGetBackendExtraData(backend)

	id := data["requestId"].(string)
	token, _ := getAuth(data["auth"].(map[string]any)["data"].(map[string]any))
	backendData.resolvedBasicTokens[id] = AuthToken{token}
}

func newBearerAuthTokenManagerHandler(backend *backend, data map[string]any) {
	backendData := extrasAuthManagerGetBackendExtraData(backend)
	managerId := backend.nextId()

	manager := auth.BearerTokenManager(
		func(context.Context) (neo4j.AuthToken, *time.Time, error) {
			id := backend.nextId()
			backend.writeResponse(
				"BearerAuthTokenProviderRequest",
				map[string]any{
					"id":                       id,
					"bearerAuthTokenManagerId": managerId,
				})
			for backend.process() {
				if bearerToken, ok := backendData.resolvedBearerTokens[id]; ok {
					delete(backendData.resolvedBearerTokens, id)
					return bearerToken.token, bearerToken.expiration, nil
				}
			}
			return neo4j.AuthToken{}, nil, nil
		})
	backendData.authTokenManagers[managerId] = manager
	backend.writeResponse("BearerAuthTokenManager", map[string]any{"id": managerId})
}

func bearerAuthTokenProviderCompletedHandler(backend *backend, data map[string]any) {
	backendData := extrasAuthManagerGetBackendExtraData(backend)

	id := data["requestId"].(string)
	bearerToken := data["auth"].(map[string]any)["data"].(map[string]any)
	token, err := getAuth(bearerToken["auth"].(map[string]any)["data"].(map[string]any))
	if err != nil {
		backend.writeError(err)
		return
	}
	var expiration *time.Time
	expiresInRaw := bearerToken["expiresInMs"]
	if expiresInRaw != nil {
		expiresIn := time.Millisecond * time.Duration(asInt64(bearerToken["expiresInMs"].(json.Number)))
		expirationTime := Now().Add(expiresIn)
		expiration = &expirationTime
	}
	backendData.resolvedBearerTokens[id] = AuthTokenAndExpiration{token, expiration}
}

func authTokenManagerCloseHandler(backend *backend, data map[string]any) {
	backendData := extrasAuthManagerGetBackendExtraData(backend)

	id := data["id"].(string)
	delete(backendData.authTokenManagers, id)
	backend.writeResponse("AuthTokenManager", map[string]any{"id": id})
}

func serializeAuth(token neo4j.AuthToken) map[string]any {
	return map[string]any{
		"name": "AuthorizationToken",
		"data": token.Tokens,
	}
}
