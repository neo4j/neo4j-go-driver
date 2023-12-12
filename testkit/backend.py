"""
Executed in Go driver container.
Assumes driver and backend has been built.
Responsible for starting the test backend.
"""

import os
import subprocess
import sys


if __name__ == "__main__":
    backend_path = os.path.join(".", "testkit-backend")
    subprocess.check_call(
        [
            "go", "run", "-tags", "internal_testkit,internal_time_mock",
            "-buildvcs=false", backend_path
        ],
        stdout=sys.stdout, stderr=sys.stderr
    )
