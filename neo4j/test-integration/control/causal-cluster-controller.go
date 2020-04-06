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
	"os"
	"strconv"
)

func isClusterInstalled(at string) bool {
	if _, err := os.Stat(at); os.IsNotExist(err) {
		return false
	}

	return true
}

func installCluster(version string, cores int, readReplicas int, password string, port int, to string) error {
	_, err := executeCommand("neoctrl-cluster", "install",
		"--cores", strconv.Itoa(cores),
		"--read-replicas", strconv.Itoa(readReplicas),
		"--password", password,
		"--initial-port", strconv.Itoa(port),
		version,
		to)

	return err
}

func startCluster(at string) (string, error) {
	return executeCommand("neoctrl-cluster", "start", at)
}

func startClusterMember(at string) (string, error) {
	return executeCommand("neoctrl-start", at)
}

func stopCluster(at string) error {
	_, err := executeCommand("neoctrl-cluster", "stop", at)
	return err
}

func stopClusterMember(at string) error {
	_, err := executeCommand("neoctrl-stop", at)
	return err
}

func killCluster(at string) error {
	_, err := executeCommand("neoctrl-cluster", "stop", "--kill", at)
	return err
}

func killClusterMember(at string) error {
	_, err := executeCommand("neoctrl-stop", "--kill", at)
	return err
}
