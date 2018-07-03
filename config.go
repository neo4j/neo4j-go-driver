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
    "time"
)

// A Config contains options that can be used to customize certain
// aspects of the driver
type Config struct {
    // Whether to turn on/off TLS encryption (default: true)
    encrypted                   bool
    log                         Logging
    maxTransactionRetryDuration time.Duration
}

type ConfigBuilder struct {
    config *Config
}

func DefaultConfig() *Config {
    return &Config{
        encrypted:                   true,
        log:                         NoOpLogger(),
        maxTransactionRetryDuration: 30 * time.Second,
    }
}

func NewConfigBuilder() *ConfigBuilder {
    return &ConfigBuilder{
        config: DefaultConfig(),
    }
}

func (builder *ConfigBuilder) WithEncryption() *ConfigBuilder {
    builder.config.encrypted = true
    return builder
}

func (builder *ConfigBuilder) WithoutEncryption() *ConfigBuilder {
    builder.config.encrypted = false
    return builder
}

func (builder *ConfigBuilder) WithMaxTransactionRetryTime(duration time.Duration) *ConfigBuilder {
    builder.config.maxTransactionRetryDuration = duration
    return builder
}

func (builder *ConfigBuilder) WithLogging(logging Logging) *ConfigBuilder {
    if logging == nil {
        logging = NoOpLogger()
    }
    builder.config.log = logging
    return builder
}

func (builder *ConfigBuilder) Build() *Config {
    return builder.config
}
