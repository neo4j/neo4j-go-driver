"""
Executed in Go driver container.
Responsible for running stress tests.
Assumes driver has been setup by build script prior to this.
"""

import os

from common import (
    get_go_min_bin,
    run_go,
)


ROOT_PACKAGE = "github.com/neo4j/neo4j-go-driver"


if __name__ == "__main__":
    go_bin = get_go_min_bin()
    uri = "%s://%s:%s" % (
            os.environ["TEST_NEO4J_SCHEME"],
            os.environ["TEST_NEO4J_HOST"],
            os.environ["TEST_NEO4J_PORT"])
    user = os.environ["TEST_NEO4J_USER"]
    password = os.environ["TEST_NEO4J_PASS"]
    duration = os.environ.get("TEST_NEO4J_STRESS_DURATION", "20")

    # Run the stress tests
    stressPath = os.path.join(".", "test-stress")
    cmd = ["run", "-buildvcs=false", "--race", stressPath,
           "--seconds", duration,
           "-uri", uri, "-user", user, "-password", password]
    if os.environ.get("TEST_NEO4J_IS_CLUSTER"):
        cmd.append("-cluster")
    run_go(cmd, go_bin=go_bin)
