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
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package neo4j

import "time"

type TransactionConfig struct {
	Timeout  time.Duration
	Metadata map[string]interface{}
}

func WithTransactionTimeout(timeout time.Duration) func(*TransactionConfig) {
	return func(config *TransactionConfig) {
		config.Timeout = timeout
	}
}

func WithTransactionMetadata(metadata map[string]interface{}) func(*TransactionConfig) {
	return func(config *TransactionConfig) {
		config.Metadata = metadata
	}
}
