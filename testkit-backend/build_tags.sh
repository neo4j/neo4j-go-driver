#!/usr/bin/env bash

set -Eeuo pipefail

vergte() {
	[ "$1" = "$(echo -e "$1\n$2" | sort -Vr | head -n1)" ]
}

vergt() {
	[ "$1" = "$2" ] && return 1 || verlte $1 $2
}

version="${1:-''}"

if [ -z "$version" ]; then
	# choose a version bigger than any version checked below
	# => next major version, at which point we can clean-up this script as it only needs to support the current major
	version="6.0.0"
elif [ $(echo "$version" | cut -d "." -f 1) != "5" ]; then
	echo "Script only works for 5.x" >&2
	exit 1
fi

if vergte "$version" "5.28.1"; then
	tags="internal_neo4j_go_driver_testkit,internal_neo4j_go_driver_time_mock"
else
	tags="internal_testkit,internal_time_mock"
fi

if vergte "$version" "5.28.0"; then
	tags="$tags,internal_neo4j_testkit_dns_resolver,internal_neo4j_testkit_home_db_cache"
fi

echo $tags
