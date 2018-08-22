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

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func neo4jVersionAndEditionToTestAgainst() string {
	if val, ok := os.LookupEnv("NEOCTRLARGS"); ok {
		return val
	}

	return "-e 3.4.6"
}

func neo4jVersionToTestAgainst() string {
	if val, ok := os.LookupEnv("NEOCTRLARGS"); ok {
		return strings.TrimSpace(strings.Replace(val, "-e", "", -1))
	}

	return "3.4.6"
}

func neo4jEditionToTestAgainst() string {
	if val, ok := os.LookupEnv("NEOCTRLARGS"); ok {
		if strings.Index(val, "-e") < 0 {
			return ""
		}
	}

	return "-e"
}

func executeCommand(command string, arguments ...string) (string, error) {
	var stdoutBuf, stderrBuf bytes.Buffer

	cmd := exec.Command(command, arguments...)
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	err := cmd.Run()
	if err != nil {
		return "", fmt.Errorf("command execution (program: %s, arguments: %v) failed with error %s", command, arguments, stderrBuf.String())
	}

	return stdoutBuf.String(), nil
}
