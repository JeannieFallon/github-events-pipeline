#!/usr/bin/env python3
from __future__ import annotations

import sys
import pathlib
import argparse

import pandas

def log(msg: string):
    print()
    print(f">>> {msg}")


def run(infile: pathlib.Path, outdir: pathlib.Path) -> pathlib.Path:
    pass


def main() -> int:
    parser = argparse.ArgumentParser(description="GitHub events ETL with Pandas")
    parser.add_argument("--infile", "-i", default="data/events.csv", type=pathlib.Path)
    parser.add_argument("--outdir", "-o", default="exports", type=pathlib.Path)

    args = parser.parse_args()

    log(f"Running pipeline for {args.infile} . . .")
    out = run(args.infile, args.outdir)

    return 0


if __name__ == "__main__":
    sys.exit(main())
