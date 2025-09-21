from pathlib import Path

import pandas as pd

import pipeline.core as pipeline

def test_pipeline(tmp_path: Path):
    df = pd.DataFrame({
        "ts": pd.to_datetime(["2024-01-01T00:00:00Z","2024-01-01T01:00:00Z"]),
        "repo": ["A/B","A/B"],
        "actor": ["u1","u2"],
        "event_type": ["PushEvent","PushEvent"],
    })
    inp = tmp_path / "events.csv"
    df.to_csv(inp, index=False)
    out = pipeline.run(inp, tmp_path)
    got = pd.read_csv(out)

    assert {"day","event_type","event_count"} <= set(got.columns)
    assert got["event_count"].sum() == 2

