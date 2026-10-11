# Tune performance

Goal: speed up a large run or make the collection fit on a machine with
limited memory.

## The three main levers

| CLI option | Role | Recommendation |
|---|---|---|
| `--threads` | Number of DuckDB threads for SQL queries | Start at `4`; beyond that, gains depend on storage |
| `--duckdb-memory-limit` | Total memory allowed for DuckDB (default `10GB`) | Do not exceed ~70% of the machine RAM |
| `--duckdb-max-temp-directory-size` | Max disk space for temporary files (default `50GB`) | Size it according to the COG volume |

## Example: 16 GB RAM machine

```bash
poetry run geo_data_collector \
    --threads 4 \
    --duckdb-memory-limit "8GB" \
    --duckdb-max-temp-directory-size "30GB" \
    --working-directory "/mnt/fast-disk/output" \
    ...
```

## How these parameters influence results

- **Memory too low**: DuckDB spills to disk (`temp_directory` = the working
  directory) — the run completes but is significantly slower.
- **Temporary disk full**: the current step fails with an explicit error; the
  working directory is kept for diagnosis.
- **Threads**: only affects speed, never the content of the outputs.

## Log to diagnose

```bash
poetry run geo_data_collector --loglevel DEBUG --log-file collection.log ...
```

The log file keeps the trace of queries and durations, including when the
terminal crashes.
