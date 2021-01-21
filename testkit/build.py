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
    run(["go", "build", "-v", "./..."])

    # Compile for 32 bits ARM to make sure it builds
    print("Building for 32 bits")
    arm32Env = os.environ.copy()
    arm32Env["GOOS"] = "linux"
    arm32Env["GOARCH"] = "arm"
    arm32Env["GOARM"] = "7"
    run(["go", "build", "./..."], env=arm32Env)
