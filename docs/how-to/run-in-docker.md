# Run in Docker

Goal: run the collector in a container, without a local Python
environment.

## Build the image

From the project root:

```bash
docker build -t geo-collect .
```

The image is a two-stage build: a builder stage installs the main
dependencies with Poetry (the `dev` and `docs` groups are excluded), the
runtime stage contains only the virtualenv at `/opt/venv`, an unprivileged
`collector` user, and the DuckDB ICU/JSON extensions pre-installed (no
network access needed at run time for extensions).

### Use the CI-built image

The `.github/workflows/docker.yml` workflow publishes the image to GitHub
Container Registry on every push to `main`:

```bash
docker pull ghcr.io/stroppycow/geo-collect:latest
```

Available tags: `latest` and `main` (default branch), `pr-<number>`
(pull request builds, not pushed), `<version>` and `<major>.<minor>`
(for `v*` Git tags). Pull requests build the image as a check only.

## Run the collection

Mount the configuration files and an output directory, then pass the usual
CLI options:

```bash
docker run --rm \
    -v "$(pwd)/config:/data/config" \
    -v "$(pwd)/output:/data/output" \
    geo-collect \
    --acquisition-config-file /data/config/config-acquisition.yaml \
    --exceptions-handler-config-file /data/config/config-exceptions-handler.yaml \
    --working-directory /data/output
```

Without arguments, the container prints the CLI help (`docker run geo-collect`).

## Container specifics

| Aspect | Detail |
|---|---|
| Working directory | `/data` (intended mount point for configs and outputs) |
| Runtime user | `collector` (uid 1000), unprivileged |
| DuckDB extensions | ICU and JSON pre-installed in the image |
| Performance options | `--threads`, `--duckdb-memory-limit` work as usual; the container memory limit can be set with `docker run --memory` |

## Points of attention

- **File permissions**: the container writes as uid 1000. If your host
  output directory belongs to another uid, pre-create it with `chown` or
  run with `--user "$(id -u):$(id -g)"`.
- **Network**: the collection itself queries the INSEE and LaPoste
  endpoints, so the container needs outbound network access.
- **Tags**: for a repeatable build, tag with the version:
  `docker build -t geo-collect:0.0.1 .`
