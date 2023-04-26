import os
import subprocess
import sys


def run(args):
    subprocess.run(
        args, universal_newlines=True, check=True,
        stdout=sys.stdout, stderr=sys.stderr,
    )


if __name__ == "__main__":
    package = os.path.join(".", "neo4j", "test-integration", "...")
    cmd = ["go", "test", "-buildvcs=false"]
    if os.environ.get("TEST_IN_TEAMCITY", False):
        cmd = cmd + ["-v", "-json"]
    run(cmd + [package])
