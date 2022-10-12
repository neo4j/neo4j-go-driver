"""
Executed in Go driver container.
Responsible for building driver and test backend.
"""
from pathlib import Path
import os
import subprocess


def run(args, env=None):
    subprocess.run(args, universal_newlines=True, stderr=subprocess.STDOUT,
                   check=True, env=env)


if __name__ == "__main__":
    # When running inside docker, pwd (this directory containing the git repo)
    # will be mounted into the container and it's not owned by root but whatever
    # UID the TeamCity agent process has. Hence, we need to include this
    # directory to the ones trusted by git (CVE-2022-24765).
    # This allows Go to use VCS stamping
    print("Include current folder to Git safe directories", flush=True)

    currentDir = subprocess.check_output(["pwd"]).decode("utf-8").strip()
    run(["git", "config", "--global", "--add", "safe.directory", currentDir])

    print("Building for current target", flush=True)
    run(["go", "build", "-v", "./..."])

    # Compile for 32 bits ARM to make sure it builds
    print("Building for 32 bits", flush=True)
    arm32Env = os.environ.copy()
    arm32Env["GOOS"] = "linux"
    arm32Env["GOARCH"] = "arm"
    arm32Env["GOARM"] = "7"
    run(["go", "build", "./..."], env=arm32Env)

    print("Vet sources", flush=True)
    run(["go", "vet", "./..."])

    print("Install staticcheck", flush=True)
    run(["go", "install", "honnef.co/go/tools/cmd/staticcheck@latest"])

    print("Run staticcheck", flush=True)
    gopath = Path(
        subprocess.check_output(["go", "env", "GOPATH"]).decode("utf-8").strip()
    )
    run([str(gopath / "bin" / "staticcheck"), "./..."])
