"""
Executed in Go driver container.
Responsible for building driver and test backend.
"""

import sys
from pathlib import Path
import os
import subprocess

from common import (
    get_go_min_bin,
    run,
)


if __name__ == "__main__":
    defaultEnv = os.environ.copy()
    defaultEnv["GOFLAGS"] = "-buildvcs=false"
    go_bin = get_go_min_bin()

    print("Building for current target", flush=True)
    run(
        [
            go_bin, "build", "-tags",
            "internal_testkit,internal_time_mock", "-v", "./..."
        ],
        env=defaultEnv
    )

    # Compile for 32 bits ARM to make sure it builds
    print("Building for 32 bits", flush=True)
    arm32Env = defaultEnv.copy()
    arm32Env["GOOS"] = "linux"
    arm32Env["GOARCH"] = "arm"
    arm32Env["GOARM"] = "7"
    run([go_bin, "build", "./neo4j/..."], env=arm32Env)

    print("Vet sources", flush=True)
    run(
        [
            go_bin, "vet", "-tags", "internal_testkit,internal_time_mock",
            "./..."
        ],
        env=defaultEnv
    )

    print("Install staticcheck", flush=True)
    run([go_bin, "install", "honnef.co/go/tools/cmd/staticcheck@v0.3.3"],
        env=defaultEnv)

    print("Run staticcheck", flush=True)
    gopath = Path(
        subprocess.check_output([go_bin, "env", "GOPATH"]).decode("utf-8").strip()
    )
    run(
        [
            str(gopath / "bin" / "staticcheck"),
            "-tags", "internal_testkit,internal_time_mock",
            "./..."
        ],
        env=defaultEnv
    )
