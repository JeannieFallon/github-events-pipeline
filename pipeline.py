#!/usr/bin/env python3
from __future__ import annotations

import sys
import pathlib
import argparse
from typing import Final
from datetime import datetime

import pandas as pd


TIMESTAMP = datetime.now().strftime("%Y%m%d-%H%M%S")
COLS: Final = {"ts", "repo", "actor", "event_type"}
DTYPES: Final = {
    "repo": "string",
    "actor": "string",
    "event_type": "string",
}


"""Logger & error hanlder to standardize script output"""


def log(msg: string) -> None:
    print(f"\n>>> {msg}")


def err(msg: string) -> None:
    print(f"\n!!! {msg}", file=sys.stderr)


"""Use pandas to get data frame from input CSV file"""


def ingest(infile: pathlib.Path) -> pd.DataFrame:
    try:
        data_frame = pd.read_csv(infile, dtype=DTYPES, parse_dates=["ts"])
    except pd.errors.EmptyDataError:
        raise ValueError(f"{infile} is empty or has no columns")

    if data_frame.empty:
        raise ValueError(f"{infile} contained headers but no rows of data")

    missing = COLS - set(data_frame.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    return data_frame


"""Call ingest, manage outdir creation, and write data frame to outfile"""


def run(infile: pathlib.Path, outdir: pathlib.Path) -> pathlib.Path:
    data_frame = ingest(infile)
    log(f"Ingested {len(data_frame):,} rows with columns {list(data_frame.columns)}")

    outdir.mkdir(parents=True, exist_ok=True)
    outfile = outdir / f"ingested_{TIMESTAMP}.csv"
    data_frame.to_csv(outfile, index=False)

    return outfile


"""Handle basic arg parsing"""


def main() -> int:
    parser = argparse.ArgumentParser(description="GitHub events ETL with Pandas")
    parser.add_argument("--infile", "-i", default="data/events.csv", type=pathlib.Path)
    parser.add_argument("--outdir", "-o", default="exports", type=pathlib.Path)

    args = parser.parse_args()

    log(f"Running pipeline for {args.infile} . . .")
    try:
        outfile = run(args.infile, args.outdir)
    except (ValueError, FileNotFoundError) as e:
        err(str(e))
        return 1

    log(f"Success! Results written to: {outfile} . . .")
    return 0


if __name__ == "__main__":
    sys.exit(main())
