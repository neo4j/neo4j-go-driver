/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"path"
	"strings"
	"sync"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

// SingleInstance holds information about the single instance server
type SingleInstance struct {
	path      string
	authToken neo4j.AuthToken
	config    configFunc
	boltURI   string
}

var singleInstance *SingleInstance
var singleInstanceErr error
var singleInstanceLock sync.Mutex

// EnsureSingleInstance either returns an existing server instance or starts up a new one
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

// StopSingleInstance stops the server
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

	boltURI := ""
	scanner := bufio.NewScanner(strings.NewReader(output))
	scannerIndex := 0
	for scanner.Scan() {
		if scannerIndex == 1 {
			boltURI = scanner.Text()
		}

		scannerIndex++
	}
	if err = scanner.Err(); err != nil {
		return nil, err
	}

	authToken := neo4j.BasicAuth(username, password, "")
	config := func(config *neo4j.Config) {
		config.Encrypted = useEncryption()
		config.Log = neo4j.ConsoleLogger(logLevel())
	}

	result := &SingleInstance{
		path:      path,
		authToken: authToken,
		config:    config,
		boltURI:   boltURI,
	}

	if err = result.deleteData(); err != nil {
		return result, err
	}

	return result, nil
}

// Path returns the folder where the server is installed
func (server *SingleInstance) Path() string {
	return server.path
}

// TLSCertificate returns the installed certificate used by the server
func (server *SingleInstance) TLSCertificate() *x509.Certificate {
	bytes, err := ioutil.ReadFile(path.Join(server.path, "neo4jhome", "certificates", "neo4j.cert"))
	if err != nil {
		return nil
	}

	der, _ := pem.Decode(bytes)
	if der == nil {
		return nil
	}

	cert, err := x509.ParseCertificate(der.Bytes)
	if err != nil {
		return nil
	}

	return cert
}

// BoltURI returns the bolt uri used to connect to the member
func (server *SingleInstance) BoltURI() string {
	return server.boltURI
}

// Username returns the configured username
func (server *SingleInstance) Username() string {
	return username
}

// Password returns the configured password
func (server *SingleInstance) Password() string {
	return password
}

// AuthToken returns the configured authentication token
func (server *SingleInstance) AuthToken() neo4j.AuthToken {
	return server.authToken
}

// Config returns the configured configurer function
func (server *SingleInstance) Config() func(config *neo4j.Config) {
	return server.config
}

// Driver returns a driver instance to the server
func (server *SingleInstance) Driver() (neo4j.Driver, error) {
	return neo4j.NewDriver(server.boltURI, server.authToken, server.config)
}

func (server *SingleInstance) deleteData() error {
	driver, err := server.Driver()
	if err != nil {
		return err
	}

	return deleteData(driver)
}
