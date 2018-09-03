/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package control

import (
	"bufio"
	"strings"
	"sync"

	"github.com/neo4j/neo4j-go-driver/neo4j"
)

type SingleInstance struct {
	path      string
	authToken neo4j.AuthToken
	config    configFunc
	boltUri   string
}

var singleInstance *SingleInstance
var singleInstanceErr error
var singleInstanceLock sync.Mutex

func EnsureSingleInstance() (*SingleInstance, error) {
	if singleInstance == nil && singleInstanceErr == nil {
		singleInstanceLock.Lock()
		defer singleInstanceLock.Unlock()
		if singleInstance == nil {
			singleInstance, singleInstanceErr = newSingleInstance(resolveServerPath(false))
		}
	}

	return singleInstance, singleInstanceErr
}

func StopSingleInstance() {
	if singleInstance != nil {
		singleInstanceLock.Lock()
		defer singleInstanceLock.Unlock()

		if singleInstance != nil {
			stopSingleInstance(singleInstance.path)
		}
	}
}

func newSingleInstance(path string) (*SingleInstance, error) {
	var output string
	var err error

	if !isSingleInstanceInstalled(path) {
		// there isn't any single instance server installation
		if err = installSingleInstance(versionToTestAgainst(), editionToTestAgainst(), password, path); err != nil {
			return nil, err
		}
	}

	if output, err = startSingleInstance(path); err != nil {
		return nil, err
	}

	boltUri := ""
	scanner := bufio.NewScanner(strings.NewReader(output))
	scannerIndex := 0
	for scanner.Scan() {
		if scannerIndex == 1 {
			boltUri = scanner.Text()
		}

		scannerIndex++
	}
	if err = scanner.Err(); err != nil {
		return nil, err
	}

	authToken := neo4j.BasicAuth(username, password, "")
	config := func(config *neo4j.Config) {
		config.Log = neo4j.ConsoleLogger(logLevel())
	}

	result := &SingleInstance{
		path:      path,
		authToken: authToken,
		config:    config,
		boltUri:   boltUri,
	}

	if err = result.deleteData(); err != nil {
		return result, err
	}

	return result, nil
}

func (server *SingleInstance) BoltUri() string {
	return server.boltUri
}

func (server *SingleInstance) Username() string {
	return username
}

func (server *SingleInstance) Password() string {
	return password
}

func (server *SingleInstance) AuthToken() neo4j.AuthToken {
	return server.authToken
}

func (server *SingleInstance) Config() func(config *neo4j.Config) {
	return server.config
}

func (server *SingleInstance) Driver() (neo4j.Driver, error) {
	return neo4j.NewDriver(server.boltUri, server.authToken, server.config)
}

func (server *SingleInstance) deleteData() error {
	driver, err := server.Driver()
	if err != nil {
		return err
	}

	return deleteData(driver)
}
