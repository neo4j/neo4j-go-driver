import copy
import os
import subprocess
import sys
from pathlib import Path


def get_go_min_bin():
    return os.environ.get('GOMINBIN', 'go')


def run(args, env=None):
    subprocess.run(
        args, universal_newlines=True, check=True, env=env,
        stdout=sys.stdout, stderr=sys.stderr,
    )


def run_output(args, env=None):
    return subprocess.check_output(args, env=env).decode("utf-8")


def run_go(args, go_bin="go", env=None):
    if env is None:
        env = os.environ.copy()
    else:
        env = copy.copy(env)
    env["GOROOT"] = run_output([go_bin, "env", "GOROOT"]).strip()
    env["PATH"] = f"{env['GOROOT']}/bin:{env['PATH']}"
    subprocess.run(
        [go_bin, *args], universal_newlines=True, check=True, env=env,
        stdout=sys.stdout, stderr=sys.stderr,
    )


def run_go_bin(cmd, args, go_bin="go", env=None):
    if env is None:
        env = os.environ.copy()
    else:
        env = copy.copy(env)
    env["GOROOT"] = run_output([go_bin, "env", "GOROOT"]).strip()
    env["PATH"] = f"{env['GOROOT']}/bin:{env['PATH']}"
    gopath = Path(run_output([go_bin, "env", "GOPATH"]).strip())
    cmd = str(gopath / "bin" / cmd)
    subprocess.run(
        [cmd, *args], universal_newlines=True, check=True, env=env,
        stdout=sys.stdout, stderr=sys.stderr,
    )
