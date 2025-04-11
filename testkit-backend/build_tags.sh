#!/usr/bin/env bash

set -Eeuo pipefail

verlte() {
    [ "$1" = "$(echo -e "$1\n$2" | sort -V | head -n1)" ]
}

verlt() {
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

if verlt "$version" "5.28.1"; then
	tags="internal_testkit,internal_time_mock"
else
	tags="internal_neo4j_go_driver_testkit,internal_neo4j_go_driver_time_mock"
fi

if verlt "$version" "5.28.0"; then
	tags="$tags,internal_neo4j_testkit_no_dns_resolver,internal_neo4j_testkit_no_home_db_cache"
else
	echo $tags
	exit 0
fi

if verlt "$version" "5.26.0"; then
	tags="$tags,internal_neo4j_testkit_no_gql_error"
else
	echo $tags
	exit 0
fi

if verlt "$version" "5.23.0"; then
	tags="$tags,internal_neo4j_testkit_no_gql_status"
else
	echo $tags
	exit 0
fi

if verlt "$version" "5.19.0"; then
	tags="$tags,internal_neo4j_testkit_no_mtls"
else
	echo $tags
	exit 0
fi

if verlt "$version" "5.18.0"; then
	tags="$tags,internal_neo4j_testkit_no_execute_query_auth"
else
	echo $tags
	exit 0
fi

echo $tags
