/*
 * Copyright (c) "Neo4j"
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
	"fmt"
	"os"
	"path"
	"strings"
)

func isSingleInstanceInstalled(at string) bool {
	if _, err := os.Stat(path.Join(at, "neo4jhome")); os.IsNotExist(err) {
		return false
	}

	return true
}

func installSingleInstance(version string, edition string, password string, to string) error {
	var err error
	var actualInstallPath string
	var args []string

	if edition == enterpriseEdition {
		args = append(args, "-e")
	}

	args = append(args, version, to)

	if actualInstallPath, err = executeCommand("neoctrl-install", args...); err != nil {
		return fmt.Errorf("unable to install neo4j database: %v", err.Error())
	}

	actualInstallPath = strings.TrimSpace(actualInstallPath)

	if _, err = executeCommand("neoctrl-set-initial-password", password, actualInstallPath); err != nil {
		return fmt.Errorf("unable to set initial password: %v", err.Error())
	}

	if err = os.Rename(actualInstallPath, path.Join(to, "neo4jhome")); err != nil {
		return fmt.Errorf("unable to rename install folder: %v", err.Error())
	}

	return nil
}

func startSingleInstance(at string) (string, error) {
	return executeCommand("neoctrl-start", path.Join(at, "neo4jhome"))
}

func stopSingleInstance(at string) error {
	_, err := executeCommand("neoctrl-stop", path.Join(at, "neo4jhome"))
	return err
}

func killSingleInstance(at string) error {
	_, err := executeCommand("neoctrl-stop", "--kill", path.Join(at, "neo4jhome"))
	return err
}
