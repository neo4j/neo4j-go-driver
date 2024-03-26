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
	"log"
	"os"
	"strconv"
)

type config struct {
	backendPort int
	neo4jHost   string
	neo4jPort   int
	neo4jScheme string
	neo4jUser   string
	neo4jPass   string
	driverDebug bool
}

func makeConfig() config {
	return config{
		backendPort: getEnv("TEST_BACKEND_PORT", 9000),
		neo4jHost:   getEnv("TEST_NEO4J_HOST", "localhost"),
		neo4jPort:   getEnv("TEST_NEO4J_PORT", 7687),
		neo4jScheme: getEnv("TEST_NEO4J_SCHEME", "neo4j"),
		neo4jUser:   getEnv("TEST_NEO4J_USER", "neo4j"),
		neo4jPass:   getEnv("TEST_NEO4J_PASS", "password"),
		driverDebug: getEnv("TEST_DRIVER_DEBUG", false),
	}
}

// getEnv is a generic function to get an environment variable and convert it to the type T.
// defaultValue is used if the environment variable is not set or if conversion fails.
func getEnv[T any](key string, defaultValue T) T {
	valueStr, exists := os.LookupEnv(key)
	if !exists {
		return defaultValue
	}

	var value T
	switch any(value).(type) {
	case string:
		return any(valueStr).(T)
	case int:
		intValue, err := strconv.Atoi(valueStr)
		if err != nil {
			log.Printf("Warning: Failed to convert %s to int, using default value. Error: %v", key, err)
			return defaultValue
		}
		return any(intValue).(T)
	case bool:
		boolValue, err := strconv.ParseBool(valueStr)
		if err != nil {
			log.Printf("Warning: Failed to convert %s to bool, using default value. Error: %v", key, err)
			return defaultValue
		}
		return any(boolValue).(T)
	default:
		log.Printf("Warning: Unsupported type for environment variable conversion.")
		return defaultValue
	}
}
