"""
Executed in Go driver container.
Responsible for building driver and test backend.
"""
import os
import subprocess


def run(args, env=None):
    subprocess.run(args, universal_newlines=True, stderr=subprocess.STDOUT,
                   check=True, env=env)


if __name__ == "__main__":
    print("Building for current target")
    run(["go", "build", "-buildvcs=false", "-v", "./..."])

    # Compile for 32 bits ARM to make sure it builds
    print("Building for 32 bits")
    arm32Env = os.environ.copy()
    arm32Env["GOOS"] = "linux"
    arm32Env["GOARCH"] = "arm"
    arm32Env["GOARM"] = "7"
    run(["go", "build", "-buildvcs=false", "./..."], env=arm32Env)

    print("Vet sources")
    run(["go", "vet", "-buildvcs=false", "./..."])

    print("Statically check sources")
    run(["go", "install", "honnef.co/go/tools/cmd/staticcheck@latest"])
    run(["staticcheck", "./..."])
