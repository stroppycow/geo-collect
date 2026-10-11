# Handle COG exceptions

Goal: ignore or correct a known INSEE/LaPoste data anomaly without waiting
for an upstream fix.

## When to use this mechanism

The pipeline applies strict quality checks (URI uniqueness, date
consistency, parent existence...). Some COG releases contain known anomalies
that would block the collection. The `config-exceptions-handler.yaml` file
lets you declare, per entity type, exceptions: either to **ignore** (the
check does not apply to that URI) or to **correct** (a replacement value is
provided).

## Declare an exception

```yaml title="config/config-exceptions-handler.yaml"
insee:
  communes:
    root:
      - uri: "http://id.insee.fr/geo/commune/xxxxxxx"
        action: "remove"          # ignore this entity
  departements:
    root: []                       # no exception on departements
```

Each `root` list accepts `AddOrReplace` objects (correction: URI
+ replacement fields) or `GeoRemove` (removal). Declared URIs must be
**unique** per list — a pydantic validator enforces it.

## Apply the correction at run time

```bash
poetry run geo_data_collector \
    --exceptions-handler-config-file "./config/config-exceptions-handler.yaml" \
    ...
```

## Check the results

Removed entities are written to the `remove/` sub-directory of the relevant
step, added or replaced entities to `add_or_replace/`: every applied
exception keeps an auditable trace.

```bash
cat output/01_processing/remove/communes.csv
```

## Points of attention

- An exception declares a **management decision**: document it (YAML comment
  + ticket) so it can be dropped once the anomaly is fixed upstream.
- The same mechanism exists for LaPoste (`laposte.root`: manual mapping of an
  Hexasmal label to an INSEE entity).
- For the full object schemas, see
  [Configuration](../reference/configuration.md).
