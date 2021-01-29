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
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strings"

	"github.com/neo4j/neo4j-go-driver/neo4j"
)

func versionAndEditionToTestAgainst() string {
	if val, ok := os.LookupEnv("NEOCTRLARGS"); ok {
		return val
	}

	return defaultVersionAndEdition
}

func versionToTestAgainst() string {
	return strings.TrimSpace(strings.Replace(versionAndEditionToTestAgainst(), "-e", "", -1))
}

func editionToTestAgainst() string {
	if strings.Index(versionAndEditionToTestAgainst(), "-e") < 0 {
		return "community"
	}

	return "enterprise"
}

func logLevel() neo4j.LogLevel {
	if val, ok := os.LookupEnv("NEOLOGLEVEL"); ok {
		switch strings.ToLower(val) {
		case "error":
			return neo4j.ERROR
		case "warning":
			return neo4j.WARNING
		case "info":
			return neo4j.INFO
		case "debug":
			return neo4j.DEBUG
		}
	}

	return defaultLogLevel
}

func resolveServerPath(isCluster bool) string {
	var serverPath = os.TempDir()

	if _, file, _, ok := runtime.Caller(1); ok {
		serverPath = path.Join(path.Dir(file), "..", "..", "..", "build", "server")
	}

	if isCluster {
		serverPath = path.Join(serverPath, "cluster", versionToTestAgainst())
	} else {
		serverPath = path.Join(serverPath, "single-instance", fmt.Sprintf("%s-%s", versionToTestAgainst(), editionToTestAgainst()))
	}

	return serverPath
}

func executeCommand(command string, arguments ...string) (string, error) {
	var stdoutBuf, stderrBuf bytes.Buffer

	cmd := exec.Command(command, arguments...)
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	err := cmd.Run()
	if err != nil {
		return "", fmt.Errorf("command execution (%v) failed with error %s", cmd.Args, stderrBuf.String())
	}

	return stdoutBuf.String(), nil
}

func deleteData(driver neo4j.Driver) error {
	session, err := driver.Session(neo4j.AccessModeWrite)
	if err != nil {
		return err
	}
	defer session.Close()

	for {
		result, err := session.Run("MATCH (n) WITH n LIMIT 10000 DETACH DELETE n RETURN count(n)", nil)
		if err != nil {
			return err
		}

		if result.Next() {
			deleted := result.Record().GetByIndex(0).(int64)
			if deleted == 0 {
				break
			}
		}

		if err := result.Err(); err != nil {
			return err
		}
	}

	return nil
}
