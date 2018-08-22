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

import "strconv"

func installCluster(version string, cores int, readReplicas int, password string, port int, path string) error {
	_, err := executeCommand("neoctrl-cluster", "install",
		"--cores", strconv.Itoa(cores),
		"--read-replicas", strconv.Itoa(readReplicas),
		"--password", password,
		"--initial-port", strconv.Itoa(port),
		version,
		path)

	return err
}

func startCluster(path string) (string, error) {
	return executeCommand("neoctrl-cluster", "start", path)
}

func startClusterMember(path string) (string, error) {
	return executeCommand("neoctrl-start", path)
}

func stopCluster(path string) error {
	_, err := executeCommand("neoctrl-cluster", "stop", path)
	return err
}

func stopClusterMember(path string) error {
	_, err := executeCommand("neoctrl-stop", path)
	return err
}

func killCluster(path string) error {
	_, err := executeCommand("neoctrl-cluster", "stop", "--kill", path)
	return err
}

func killClusterMember(path string) error {
	_, err := executeCommand("neoctrl-stop", "--kill", path)
	return err
}
