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
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
)

type extraRequestHandlerFunc = func(backend *backend, data map[string]any)
type extraDriverConfigFunc = func(backend *backend, data map[string]any, config *config.Config) error
type extraNewDriverHandlerFunc = func(backend *backend, driver neo4j.DriverWithContext, data map[string]any) error

var extraBlockedTestKitFeatures = make(map[string]any)
var extraTestSkips = make(map[string]string)
var extraRequestHandlers = make(map[string]extraRequestHandlerFunc)
var extraDriverConfigs = make([]extraDriverConfigFunc, 0)
var extraNewDriverHandlers = make([]extraNewDriverHandlerFunc, 0)

type ExtraRegisterEntry struct {
	newBackendExtraData         func() any
	extraBlockedTestKitFeatures []string
	extraTestSkips              map[string]string
	extraRequestHandlers        map[string]extraRequestHandlerFunc
	extraDriverConfig           extraDriverConfigFunc
	extraNewDriverHandler       extraNewDriverHandlerFunc
}

var extraRegister = make(map[string]ExtraRegisterEntry)

func registerExtra(name string, entry ExtraRegisterEntry) {
	// if extraRegister contains name
	if _, ok := extraRegister[name]; ok {
		panic("Extra name '" + name + "' already registered")
	}
	extraRegister[name] = entry

	for _, feature := range entry.extraBlockedTestKitFeatures {
		if _, ok := extraBlockedTestKitFeatures[feature]; ok {
			panic("Extra TestKit feature '" + feature + "' already blocked")
		}
		extraBlockedTestKitFeatures[feature] = struct{}{}
	}

	for testPattern, reason := range entry.extraTestSkips {
		if _, ok := extraTestSkips[testPattern]; ok {
			panic("Extra test reason '" + testPattern + "' already registered")
		}
		extraTestSkips[testPattern] = reason
	}

	for msgName, handler := range entry.extraRequestHandlers {
		if _, ok := extraRequestHandlers[msgName]; ok {
			panic("Extra request handler '" + msgName + "' already registered")
		}
		extraRequestHandlers[msgName] = handler
	}

	if entry.extraDriverConfig != nil {
		extraDriverConfigs = append(extraDriverConfigs, entry.extraDriverConfig)
	}

	if entry.extraNewDriverHandler != nil {
		extraNewDriverHandlers = append(extraNewDriverHandlers, entry.extraNewDriverHandler)
	}
}

func newBackendExtraData() map[string]any {
	extraData := make(map[string]any)
	for key, entry := range extraRegister {
		extraData[key] = entry.newBackendExtraData()
	}
	return extraData
}

func getBackendExtraData(backend *backend, name string) any {
	return backend.extrasData[name]
}
