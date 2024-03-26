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
	"fmt"
	"log"
	"net/http"
)

func main() {
	// Create backend and initialize configuration
	backend := &backend{config: makeConfig()}

	// Define endpoints
	http.HandleFunc("/ready", backend.readyHandler)
	http.HandleFunc("/workload", backend.workloadHandler)
	http.HandleFunc("/workload/", backend.workloadWithIdHandler)

	// Start server
	log.Printf("Starting server on port %d", backend.config.backendPort)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", backend.config.backendPort), nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
