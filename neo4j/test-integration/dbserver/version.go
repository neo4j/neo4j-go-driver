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
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dbserver

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

var (
	// V340 identifies server version 3.4.0
	V340 = VersionOf("3.4.0")
	// V350 identifies server version 3.5.0
	V350 = VersionOf("3.5.0")
)

func versionOfDriver(driver neo4j.Driver) Version {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close()

	result, err := session.Run("RETURN 1", nil)
	if err != nil {
		panic(err)
	}

	summary, err := result.Consume()
	if err != nil {
		panic(err)
	}

	return VersionOf(summary.Server().Version())
}

const (
	versionPattern = "(Neo4j/)?(\\d+)\\.(\\d+)(?:\\.)?(\\d*)(\\.|-|\\+)?([0-9A-Za-z-.]*)?"
	versionInDev   = "Neo4j/dev"
)

var (
	versionMatcher *regexp.Regexp
)

type Version struct {
	major int
	minor int
	patch int
}

var (
	noVersion      Version = Version{-1, -1, -1}
	inDevVersion   Version = Version{0, 0, 0}
	defaultVersion Version = Version{3, 0, 0}
)

func compareInt(num1 int, num2 int) int {
	if num1 == num2 {
		return 0
	}

	if num1 > num2 {
		return 1
	}

	return -1
}

func compareVersions(version1 Version, version2 Version) int {
	comp := compareInt(version1.major, version2.major)
	if comp == 0 {
		comp = compareInt(version1.minor, version2.minor)
		if comp == 0 {
			comp = compareInt(version1.patch, version2.patch)
		}
	}

	return comp
}

func VersionOf(server string) Version {
	if server == "" {
		return defaultVersion
	} else {
		if versionMatcher == nil {
			versionMatcher = regexp.MustCompile(versionPattern)
		}
		matches := versionMatcher.FindStringSubmatch(server)
		if matches != nil {
			major, _ := strconv.Atoi(matches[2])
			minor, _ := strconv.Atoi(matches[3])
			patch, _ := strconv.Atoi(matches[4])

			return Version{major, minor, patch}
		} else if server == versionInDev {
			return inDevVersion
		}
	}

	return noVersion
}

func (version Version) String() string {
	return fmt.Sprintf("%d.%d.%d", version.major, version.minor, version.patch)
}

func (version Version) Equals(other Version) bool {
	return compareVersions(version, other) == 0
}

func (version Version) GreaterThan(other Version) bool {
	return compareVersions(version, other) > 0
}

func (version Version) GreaterThanOrEqual(other Version) bool {
	return compareVersions(version, other) >= 0
}

func (version Version) LessThan(other Version) bool {
	return compareVersions(version, other) < 0
}

func (version Version) LessThanOrEqual(other Version) bool {
	return compareVersions(version, other) <= 0
}
