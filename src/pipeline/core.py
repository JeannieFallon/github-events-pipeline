#!/usr/bin/env python3
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


"""Logger & error handler to standardize script output"""


def log(msg: str) -> None:
    print(f"\n>>> {msg}")


def err(msg: str) -> None:
    print(f"\n!!! {msg}", file=sys.stderr)


"""ETL pipeline functions"""


def ingest(infile: pathlib.Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(infile, dtype=DTYPES, parse_dates=["ts"])
    except pd.errors.EmptyDataError:
        raise ValueError(f"{infile} is empty or has no columns")

    if df.empty:
        raise ValueError(f"{infile} contained headers but no rows of data")

    missing = COLS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    return df


def clean_normalize(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)

    # Make sure timestamp is datetime
    if not pd.api.types.is_datetime64_any_dtype(df["ts"]):
        df = df.copy()
        df["ts"] = pd.to_datetime(df["ts"], errors="coerce")

    # Drop incomplete rows (missing required cols)
    df = df.dropna(subset=list(COLS)).copy()

    # Normalize by transforming to lowercase and stripping whitespace
    df["repo"] = df["repo"].str.strip().str.lower()
    df["event_type"] = df["event_type"].str.strip()

    after = len(df)
    dropped = before - after
    log(f"Cleaned: kept {after:,} rows (dropped {dropped:,})")

    return df


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["day"] = df["ts"].dt.date
    out = (
        df.groupby(["day", "event_type"], dropna=False)
        .size()
        .reset_index(name="event_count")
        .sort_values(["day", "event_type"])
        .reset_index(drop=True)
    )

    log(f"Aggregated to {len(out):,} rows (days * event_types)")
    return out


def save(df: pd.DataFrame, outdir: pathlib.Path) -> pathlib.Path:
    outdir.mkdir(parents=True, exist_ok=True)
    outfile = outdir / f"summary_{TIMESTAMP}.csv"
    df.to_csv(outfile, index=False)
    log(f"Wrote {outfile}")
    return outfile


"""Top-level run function to manage logic flow through ETL funcs"""


def run(infile: pathlib.Path, outdir: pathlib.Path) -> pathlib.Path:
    # Extract: get dataframe from input CSV file
    df = ingest(infile)
    log(f"Ingested {len(df):,} rows with columns {list(df.columns)}")

    # Transform: clean, normalize, and aggregate data
    df = clean_normalize(df)
    log(f"Post-clean rows: {len(df):,}")
    summary = aggregate(df)

    # Load: save transformed data to output CSV file
    return save(summary, outdir)


def main() -> int:
    parser = argparse.ArgumentParser(description="GitHub events ETL with Pandas")
    parser.add_argument("--infile", "-i", default="data/events.csv", type=pathlib.Path)
    parser.add_argument("--outdir", "-o", default="exports", type=pathlib.Path)

    args = parser.parse_args()

    log(f"Running pipeline for {args.infile} . . .")
    try:
        # Run ETL data pipeline
        outfile = run(args.infile, args.outdir)
    except (ValueError, FileNotFoundError) as e:
        err(str(e))
        return 1

    log(f"Success! Results written to: {outfile} . . .")
    return 0


if __name__ == "__main__":
    sys.exit(main())
