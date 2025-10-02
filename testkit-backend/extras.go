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
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/config"
)

type extrasRequestHandlerFunc = func(backend *backend, data map[string]any)
type extrasDriverConfigFunc = func(backend *backend, data map[string]any, config *config.Config) error
type extrasNewDriverHandlerFunc = func(backend *backend, data map[string]any, driver neo4j.Driver) error
type extrasExecuteQueryConfigFunc = func(backend *backend, data map[string]any, config *neo4j.ExecuteQueryConfiguration) error
type extrasSessionConfigFunc = func(backend *backend, data map[string]any, config *neo4j.SessionConfig) error

var extrasBlockedTestKitFeatures = make(map[string]any)
var extrasTestSkips = make(map[string]string)
var extrasRequestHandlers = make(map[string]extrasRequestHandlerFunc)
var extrasDriverConfigurers = make([]extrasDriverConfigFunc, 0)
var extrasNewDriverHandlers = make([]extrasNewDriverHandlerFunc, 0)
var extrasExecuteQueryConfigurers = make([]extrasExecuteQueryConfigFunc, 0)
var extrasSessionConfigurers = make([]extrasSessionConfigFunc, 0)

type ExtrasRegisterEntry struct {
	newBackendExtraData         func() any
	extraBlockedTestKitFeatures []string
	extraTestSkips              map[string]string
	extraRequestHandlers        map[string]extrasRequestHandlerFunc
	extraDriverConfigurer       extrasDriverConfigFunc
	extraNewDriverHandler       extrasNewDriverHandlerFunc
	extraExecuteQueryConfigurer extrasExecuteQueryConfigFunc
	extraSessionConfigurer      extrasSessionConfigFunc
}

var extrasRegister = make(map[string]ExtrasRegisterEntry)

//lint:ignore U1000 Infrastructure for future expansion
func registerExtra(name string, entry ExtrasRegisterEntry) {
	if _, ok := extrasRegister[name]; ok {
		panic("Extra name '" + name + "' already registered")
	}
	extrasRegister[name] = entry

	for _, feature := range entry.extraBlockedTestKitFeatures {
		if _, ok := extrasBlockedTestKitFeatures[feature]; ok {
			panic("Extra TestKit feature '" + feature + "' already blocked")
		}
		extrasBlockedTestKitFeatures[feature] = struct{}{}
	}

	for testPattern, reason := range entry.extraTestSkips {
		if _, ok := extrasTestSkips[testPattern]; ok {
			panic("Extra test reason '" + testPattern + "' already registered")
		}
		extrasTestSkips[testPattern] = reason
	}

	for msgName, handler := range entry.extraRequestHandlers {
		if _, ok := extrasRequestHandlers[msgName]; ok {
			panic("Extra request handler '" + msgName + "' already registered")
		}
		extrasRequestHandlers[msgName] = handler
	}

	if entry.extraDriverConfigurer != nil {
		extrasDriverConfigurers = append(extrasDriverConfigurers, entry.extraDriverConfigurer)
	}

	if entry.extraNewDriverHandler != nil {
		extrasNewDriverHandlers = append(extrasNewDriverHandlers, entry.extraNewDriverHandler)
	}

	if entry.extraExecuteQueryConfigurer != nil {
		extrasExecuteQueryConfigurers = append(extrasExecuteQueryConfigurers, entry.extraExecuteQueryConfigurer)
	}

	if entry.extraSessionConfigurer != nil {
		extrasSessionConfigurers = append(extrasSessionConfigurers, entry.extraSessionConfigurer)
	}
}

func newBackendExtraData() map[string]any {
	extraData := make(map[string]any, len(extrasRegister))
	for key, entry := range extrasRegister {
		if entry.newBackendExtraData == nil {
			continue
		}
		extraData[key] = entry.newBackendExtraData()
	}
	return extraData
}

//lint:ignore U1000 Infrastructure for future expansion
func getBackendExtraData(backend *backend, name string) any {
	return backend.extrasData[name]
}
