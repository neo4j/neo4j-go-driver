#!/usr/bin/env bash

set -Eeuo pipefail

verlte() {
    [ "$1" = "$(echo -e "$1\n$2" | sort -V | head -n1)" ]
}

verlt() {
    [ "$1" = "$2" ] && return 1 || verlte "$1" "$2"
}

version="${1:-''}"

if [ -z "$version" ]; then
	# choose a version bigger than any version checked below
	# => next major version, at which point we can clean-up this script as it only needs to support the current major
	version="7.0.0"
elif [ "$(echo "$version" | cut -d "." -f 1)" != "6" ]; then
	echo "Script only works for 6.x" >&2
	exit 1
fi

if verlte "$version" "6.0.0"; then
	tags="internal_testkit,internal_time_mock"
else
	tags="internal_neo4j_go_driver_testkit,internal_neo4j_go_driver_time_mock"
fi

echo $tags
