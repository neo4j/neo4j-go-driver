#!/usr/bin/env bash

set -Eeuxo pipefail

FORMAT=${FORMAT:-' - [[%h]](https://github.com/neo4j/neo4j-go-driver/commit/%H) %s'}

get_history() {
  local start_tag
  local end_tag
  start_tag="${1}"
  end_tag="${2}"
  git log --reverse --format="${FORMAT}" "${start_tag}..${end_tag}"
}

main() {
  local start_tag
  local end_tag
  start_tag="${1:-}"
  end_tag="${2:-}"
  if [ -z "${start_tag}" ] || [ -z "${end_tag}" ]; then
    echo "${0} <start_tag> <end_tag> - missing either tag. Please specify both." >&2
    exit 1
  fi
  get_history "${start_tag}" "${end_tag}"
}

main "$@"