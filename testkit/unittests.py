"""
Executed in Go driver container.
Responsible for running unit tests.
Assumes driver has been setup by build script prior to this.
"""

import os
import subprocess
import sys

from common import (
    get_go_min_bin,
    run,
)


if __name__ == "__main__":
    # Run explicit set of unit tests to avoid running integration tests
    # Specify -v -json to make TeamCity pickup the tests
    path = os.path.join(".", "neo4j", "...")

    go_versions = {"go"}
    go_versions.add(get_go_min_bin())

    for go_bin in go_versions:
        for extra_args in (
            (), ("-tags", "internal_time_mock")
        ):
            cmd = [go_bin, "test", "-race", *extra_args]
            if os.environ.get("TEST_IN_TEAMCITY", False):
                cmd = cmd + ["-v", "-json"]
            run(cmd + ["-buildvcs=false", "-short", path])

        # Repeat racing tests
        run(cmd + ["-buildvcs=false", "-race", "-count", "50",
                   "./neo4j/internal/racing"])
