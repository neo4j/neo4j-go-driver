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

package neo4j_go_driver

import (
    "net/url"
    neo4j "neo4j-go-connector"
)

type directDriver struct {
    config    *Config
    target    url.URL
    connector neo4j.Connector
}

func configToConnectorConfig(config *Config) *neo4j.Config {
    return &neo4j.Config{
        Encryption: config.encrypted,
        Debug:      config.log.DebugEnabled(),
    }
}

func newDirectDriver(target *url.URL, token AuthToken, config *Config) (*directDriver, error) {
    if config == nil {
        config = DefaultConfig()
    }

    connector, err := neo4j.NewConnector(target.String(), token.tokens, configToConnectorConfig(config))
    if err != nil {
        return nil, err
    }

    driver := directDriver{
        config:    config,
        target:    *target,
        connector: connector,
    }
    return &driver, nil
}

func (driver *directDriver) Target() url.URL {
    return driver.target
}

func (driver *directDriver) Session() (*Session, error) {
    return driver.SessionWithAccessMode(AccessModeWrite)
}

func (driver *directDriver) SessionWithAccessMode(accessMode AccessMode) (*Session, error) {
    return driver.SessionWithAccessModeAndBookmark(accessMode)
}

func (driver *directDriver) SessionWithBookmark(bookmark string) (*Session, error) {
    return driver.SessionWithAccessModeAndBookmark(AccessModeWrite, bookmark)
}

func (driver *directDriver) SessionWithAccessModeAndBookmark(accessMode AccessMode, bookmarks ...string) (*Session, error) {
    return newSession(driver, accessMode, bookmarks), nil
}

func (driver *directDriver) Close() error {
    return driver.connector.Close()
}

func (driver *directDriver) configuration() *Config {
    return driver.config
}

func (driver *directDriver) acquire(mode AccessMode) (neo4j.Connection, error) {
    pool, err := driver.connector.GetPool()
    if err != nil {
        return nil, err
    }

    return pool.Acquire()
}
