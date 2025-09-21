# github-events-pipeline
GitHub events ETL with Pandas

## Usage

### Demo
- Run set-up script to create and load venv with Python dependencies:
```bash
source setup.sh
```
- Sample CSV file is in `data/`. Run pipeline with default sample data:
```bash
make run
```
- Expected output in timestamped file written to `exports/`:
```csv
day,event_type,event_count
2024-01-01,IssuesEvent,1
2024-01-01,PullRequestEvent,1
2024-01-01,PushEvent,1
2024-01-02,IssuesEvent,1
2024-01-03,pushEvent,1
```
- Run test to validate pipeline, using sample data:
```
make test
```

### Custom Input
- To run pipeline with custom CSV, pass as input param to script:
```bash
python -m pipeline.core --infile /path/to/file.csv
```
