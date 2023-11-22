/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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
	"math"
	"regexp"
	"strconv"
)

const (
	versionPattern = "(Neo4j/)?(\\d+)\\.(\\d+|dev)(?:\\.)?(\\d*)(\\.|-|\\+)?([0-9A-Za-z-.]*)?"
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
	noVersion      = Version{-1, -1, -1}
	defaultVersion = Version{3, 0, 0}
)

func VersionOf(server string) Version {
	if versionMatcher == nil {
		versionMatcher = regexp.MustCompile(versionPattern)
	}
	matches := versionMatcher.FindStringSubmatch(server)
	if matches != nil {
		major, _ := strconv.Atoi(matches[2])
		minor := parseMinor(matches[3])
		patch, _ := strconv.Atoi(matches[4])
		return Version{major, minor, patch}
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

func parseMinor(rawMinor string) int {
	if rawMinor == "dev" {
		return math.MaxInt
	}
	result, _ := strconv.Atoi(rawMinor)
	return result
}

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
	if comp := compareInt(version1.major, version2.major); comp != 0 {
		return comp
	}
	if comp := compareInt(version1.minor, version2.minor); comp != 0 {
		return comp
	}
	return compareInt(version1.patch, version2.patch)
}
