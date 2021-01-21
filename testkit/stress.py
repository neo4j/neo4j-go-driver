"""
Executed in Go driver container.
Responsible for running stress tests.
Assumes driver has been setup by build script prior to this.
"""
import os
import subprocess

root_package = "github.com/neo4j/neo4j-go-driver"


def run(args):
    subprocess.run(
        args, universal_newlines=True, stderr=subprocess.STDOUT, check=True)


if __name__ == "__main__":
    uri = "%s://%s:%s" % (
            os.environ["TEST_NEO4J_SCHEME"],
            os.environ["TEST_NEO4J_HOST"],
            os.environ["TEST_NEO4J_PORT"])
    user = os.environ["TEST_NEO4J_USER"]
    password = os.environ["TEST_NEO4J_PASS"]

    # Run the stress tests
    stressPath = os.path.join(".", "test-stress")
    cmd = ["go", "run", "--race", stressPath,
           "-uri", uri, "-user", user, "-password", password]
    if os.environ.get("TEST_NEO4J_IS_CLUSTER"):
        cmd.append("-cluster")
    run(cmd)
