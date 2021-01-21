"""
Executed in Go driver container.
Assumes driver and backend has been built.
Responsible for starting the test backend.
"""
import os
import subprocess

if __name__ == "__main__":
    backendPath = os.path.join(".", "testkit-backend")
    err = open("/artifacts/backenderr.log", "w")
    out = open("/artifacts/backendout.log", "w")
    subprocess.check_call(["go", "run", backendPath], stdout=out, stderr=err)
