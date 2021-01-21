"""
Executed in Go driver container.
Responsible for running unit tests.
Assumes driver has been setup by build script prior to this.
"""
import os
import subprocess


def run(args):
    subprocess.run(args, universal_newlines=True, stderr=subprocess.STDOUT,
                   check=True)


if __name__ == "__main__":
    # Run explicit set of unit tests to avoid running integration tests
    # Specify -v -json to make TeamCity pickup the tests
    cmd = ["go", "test"]
    if os.environ.get("TEST_IN_TEAMCITY", False):
        cmd = cmd + ["-v", "-json"]

    run(cmd + ["./neo4j"])
    run(cmd + ["./neo4j/internal/..."])
