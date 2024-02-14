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
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"net/http"
	"strings"
)

var ctx = context.Background()

type backend struct {
	config    config
	driver    neo4j.DriverWithContext
	workloads workloads
}

func (b *backend) readyHandler(w http.ResponseWriter, r *http.Request) {
	b.ready(w, r)
}

func (b *backend) workloadHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		b.postWorkload(w, r)
	case http.MethodPut:
		b.putWorkload(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (b *backend) workloadWithIdHandler(w http.ResponseWriter, r *http.Request) {
	// Extract the part of the URL path after "/workload/"
	workloadId := strings.TrimPrefix(r.URL.Path, "/workload/")
	if workloadId == "" {
		http.Error(w, "invalid {workloadId}", http.StatusBadRequest)
		return
	}

	// Proceed based on the method
	switch r.Method {
	case http.MethodGet:
		b.getWorkload(w, r, workloadId)
	case http.MethodPatch:
		b.patchWorkload(w, r, workloadId)
	case http.MethodDelete:
		b.deleteWorkload(w, r, workloadId)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (b *backend) ready(w http.ResponseWriter, r *http.Request) {
	// Create driver and verify connectivity
	err := b.createDriver()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Write response
	w.WriteHeader(http.StatusOK)
}

func (b *backend) postWorkload(w http.ResponseWriter, r *http.Request) {
	var request workload
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	wl, err := newWorkload(request.Method, request.Queries, request.Database, request.Routing, request.Mode)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to create workload: %v", err), http.StatusBadRequest)
		return
	}

	// Store the workload
	id := b.workloads.store(wl)

	// Set the location header
	w.Header().Set("Location", fmt.Sprintf("/workload/%s", id))
	w.WriteHeader(http.StatusCreated)
}

func (b *backend) putWorkload(w http.ResponseWriter, r *http.Request) {
	var request workload
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	wl, err := newWorkload(request.Method, request.Queries, request.Database, request.Routing, request.Mode)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to create workload: %v", err), http.StatusBadRequest)
		return
	}

	// Execute workload
	err = wl.execute(b.driver)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Write response
	w.WriteHeader(http.StatusNoContent)
}

func (b *backend) getWorkload(w http.ResponseWriter, r *http.Request, workloadId string) {
	// Get workload from our store
	workloadFromStore, ok := b.workloads.fetch(workloadId)
	if !ok {
		http.Error(w, fmt.Sprintf("workload {%s} not found", workloadId), http.StatusNotFound)
		return
	}

	// Execute workload
	err := workloadFromStore.execute(b.driver)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Write response
	w.WriteHeader(http.StatusNoContent)
}

func (b *backend) patchWorkload(w http.ResponseWriter, r *http.Request, workloadId string) {
	var request workload
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// Get workload from our store
	storedWorkload, ok := b.workloads.fetch(workloadId)
	if !ok {
		http.Error(w, fmt.Sprintf("workload {%s} not found", workloadId), http.StatusNotFound)
		return
	}

	// Patch the stored workload with the requested workload.
	if err := storedWorkload.patch(&request); err != nil {
		http.Error(w, fmt.Sprintf("failed to update workload: %v", err), http.StatusBadRequest)
		return
	}

	// Write response
	w.WriteHeader(http.StatusOK)
}

func (b *backend) deleteWorkload(w http.ResponseWriter, r *http.Request, workloadId string) {
	// Delete workload from our store
	ok := b.workloads.delete(workloadId)
	if !ok {
		http.Error(w, fmt.Sprintf("workload {%s} not found", workloadId), http.StatusNotFound)
		return
	}

	// Write response
	w.WriteHeader(http.StatusNoContent)
}

func (b *backend) createDriver() error {
	if b.driver == nil {
		uri := fmt.Sprintf("%s://%s:%d", b.config.neo4jScheme, b.config.neo4jHost, b.config.neo4jPort)
		driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(b.config.neo4jUser, b.config.neo4jPass, ""), func(config *neo4j.Config) {
			if b.config.driverDebug {
				config.Log = neo4j.ConsoleLogger(neo4j.DEBUG)
			}
		})
		if err != nil {
			return fmt.Errorf("failed to create Neo4j driver: %w", err)
		}
		if err = driver.VerifyConnectivity(ctx); err != nil {
			return fmt.Errorf("failed to connect to Neo4j: %w", err)
		}
		b.driver = driver
	}
	return nil
}
