import os

from common import (
    get_go_min_bin,
    run,
)


if __name__ == "__main__":
    package = os.path.join(".", "neo4j", "test-integration", "...")
    cmd = [get_go_min_bin(), "test", "-race", "-buildvcs=false"]
    if os.environ.get("TEST_IN_TEAMCITY", False):
        cmd = cmd + ["-v", "-json"]
    run(cmd + [package])
