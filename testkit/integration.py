import os

from common import (
    get_go_min_bin,
    run_go,
)


if __name__ == "__main__":
    go_bin = get_go_min_bin()
    package = os.path.join(".", "neo4j", "test-integration", "...")
    cmd = ["test", "-race", "-buildvcs=false"]
    if os.environ.get("TEST_IN_TEAMCITY", False):
        cmd = cmd + ["-v", "-json"]
    run_go(cmd + [package], go_bin=go_bin)
