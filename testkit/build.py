"""
Executed in Go driver container.
Responsible for building driver and test backend.
"""

import os

from common import (
    get_go_min_bin,
    run_go,
    run_go_bin,
)


if __name__ == "__main__":
    go_bin = get_go_min_bin()

    defaultEnv = os.environ.copy()
    defaultEnv["GOFLAGS"] = "-buildvcs=false"

    print("Building for current target", flush=True)
    run_go(
        [
            "build", "-tags", "internal_testkit,internal_time_mock",
            "-v", "./..."
        ],
        go_bin=go_bin,
        env=defaultEnv
    )

    # Compile for 32 bits ARM to make sure it builds
    print("Building for 32 bits", flush=True)
    arm32Env = defaultEnv.copy()
    arm32Env["GOOS"] = "linux"
    arm32Env["GOARCH"] = "arm"
    arm32Env["GOARM"] = "7"
    run_go(["build", "./neo4j/..."], go_bin=go_bin, env=arm32Env)

    print("Vet sources", flush=True)
    run_go(
        [
            "vet", "-tags", "internal_testkit,internal_time_mock",
            "./..."
        ],
        go_bin=go_bin,
        env=defaultEnv
    )

    print("Install staticcheck", flush=True)
    run_go(
        ["install", "honnef.co/go/tools/cmd/staticcheck@v0.3.3"],
        go_bin=go_bin,
        env=defaultEnv
    )

    print("Run staticcheck", flush=True)
    run_go_bin(
        "staticcheck",
        ["-tags", "internal_testkit,internal_time_mock", "./..."],
        go_bin=go_bin,
        env=defaultEnv
    )
