import os
import subprocess


def run(args):
    subprocess.run(args, universal_newlines=True, stderr=subprocess.STDOUT,
                   check=True)


if __name__ == "__main__":
    package = "./neo4j/test-integration/..."
    cmd = ["go", "test"]
    if os.environ.get("TEST_IN_TEAMCITY", False):
        cmd = cmd + ["-v", "-json"]
    run(cmd + [package])
