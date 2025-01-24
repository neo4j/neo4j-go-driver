"""
Executed in Go driver container.
Assumes driver and backend has been built.
Responsible for starting the test backend.
"""

import os


from common import (
    get_go_min_bin,
    run,
)


if __name__ == "__main__":
    backend_path = os.path.join(".", "testkit-backend")
    run(
        [
            get_go_min_bin(), "run", "-tags",
            "internal_testkit,internal_time_mock", "-buildvcs=false",
            backend_path
        ],
    )
