# syntax=docker/dockerfile:1

########## Stage 1: builder ##########
FROM python:3.13-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    POETRY_NO_INTERACTION=1 \
    # Install into the activated venv instead of creating its own.
    POETRY_VIRTUALENVS_CREATE=false

# Runtime dependencies live under [tool.poetry.dependencies], so the
# package must be installed with poetry, not plain pip.
RUN pip install "poetry==2.5.1"

# The venv is created at its final location so the COPY to the runtime
# stage keeps the embedded absolute paths valid.
# VIRTUAL_ENV is required so that poetry detects and uses this venv
# (PATH alone is not enough).
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
    VIRTUAL_ENV=/opt/venv

WORKDIR /build

COPY pyproject.toml poetry.lock README.md ./
COPY src ./src

# Main group only: dev (ipykernel, ruff, pyright) and docs groups are
# excluded from the production image.
RUN poetry install --only main

########## Stage 2: runtime ##########
FROM python:3.13-slim

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

COPY --from=builder /opt/venv /opt/venv

# Unprivileged runtime user; /data is the working directory to mount.
RUN useradd --create-home --uid 1000 collector \
    && mkdir /data \
    && chown collector:collector /data

USER collector
ENV HOME=/home/collector
WORKDIR /data

# Pre-install the DuckDB extensions (icu, json) loaded by
# utils/duckdb.py so the container does not need network access at
# runtime to fetch them.
RUN python -c "import duckdb; con = duckdb.connect(); con.install_extension('icu'); con.install_extension('json'); con.close()"

ENTRYPOINT ["geo_data_collector"]
CMD ["--help"]
