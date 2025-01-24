import os
import subprocess
import sys


def get_go_min_bin():
    return os.environ.get('GOMINBIN', 'go')


def run(args, env=None):
    subprocess.run(
        args, universal_newlines=True, check=True, env=env,
        stdout=sys.stdout, stderr=sys.stderr,
    )
