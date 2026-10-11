# Configure acquisition

Goal: choose your data sources and their connection parameters.

## The acquisition configuration file

The `--acquisition-config-file` option points to a YAML file (by default, the
values built into the code are used). Format:

```yaml title="config/config-acquisition.yaml"
insee:
  endpoint_url: "http://rdf.insee.fr/sparql"   # COG SPARQL endpoint
  backoff_factor: 0.5   # wait multiplier between two attempts
  max_retries: 5        # attempts before failure
  connect_timeout: 20   # connection timeout (s)
  read_timeout: 25      # read timeout (s)
laposte:
  endpoint_url: "https://datanova.laposte.fr/data-fair/api/v1/datasets/laposte-hexasmal/raw"
  backoff_factor: 0.5
  max_retries: 5
  connect_timeout: 20
  read_timeout: 25
```

## Parameters and their influence on results

| Parameter | Effect when increased | Effect when decreased |
|---|---|---|
| `max_retries` | Tolerates long network outages, but delays failure when the endpoint is down | Fails faster, may give up on a transient incident |
| `connect_timeout` / `read_timeout` | Tolerates slow endpoints | Errors earlier on a failing source |
| `endpoint_url` | — | Targets a mirror or an archived COG version |

## Configuration source precedence

Parameters are resolved in this order (last wins):

1. Code defaults (`InseeSupplierConfig`, `LaPosteSupplierConfig`)
2. YAML file passed to `--acquisition-config-file`
3. Environment variables, prefix `GEOCOLLECT_` with `__` separator
   (e.g. `GEOCOLLECT_INSEE__MAX_RETRIES=3`)

For field details, see [Configuration](../reference/configuration.md).

## Check that the configuration is loaded

```bash
poetry run geo_data_collector --loglevel DEBUG ... 2>&1 | grep "Loading acquisition config"
```

The log shows the loaded file path; any YAML syntax or type error (e.g. a
non-integer `max_retries`) stops the run with an explicit message.
