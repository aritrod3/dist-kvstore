#!/usr/bin/env python3
"""Run the Go load generator: go run ./cmd/benchmark (see -h). Isolated vs contended: python3 benchmark.py -compare. Output: stdout + benchmark_results.csv (gitignored)."""
import os
import subprocess
import sys


def main() -> None:
    root = os.path.dirname(os.path.abspath(__file__))
    cmd = ["go", "run", "./cmd/benchmark"] + sys.argv[1:]
    os.chdir(root)
    raise SystemExit(subprocess.call(cmd))


if __name__ == "__main__":
    main()
