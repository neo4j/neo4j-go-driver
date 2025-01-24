"""
Executed in Go driver container.
Assumes driver and backend has been built.
Responsible for starting the test backend.
"""

import os


from common import (
    get_go_min_bin,
    run_go,
)


if __name__ == "__main__":
    go_bin = get_go_min_bin()
    backend_path = os.path.join(".", "testkit-backend")
    run_go(
        [
            "run", "-tags",
            "internal_testkit,internal_time_mock", "-buildvcs=false",
            backend_path
        ],
        go_bin=go_bin,
    )
