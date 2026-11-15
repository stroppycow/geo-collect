# Step 05 — Postal codes merge

!!! abstract "Summary"
    Matches LaPoste Hexasmal labels (step 02) with INSEE entities (step 01)
    to add the `postal_codes` and `postal_codes_count` columns to the
    communes and municipal arrondissements tables, including historical
    entities.

## 1. Methodological level

### Goal

Attach to every geographic entity — present or past — the postal codes
covering it. This is the pipeline's final output for the "geolocation by
postal code" use case.

### Why this step

Hexasmal provides **no INSEE code**: the matching is done by label. Two
difficulties:

1. labels evolve (merges, renamings) — hence the event graph to propagate a
   postal code to the historical versions of the same entity;
2. one label can match several entities — hence the disambiguation rules
   (`associated_name` dedication, unique-successor propagation).

The "postal code → current commune only" alternative was rejected: it
loses all historical information, which is the very purpose of the project.

### Parameters and influence

No dedicated CLI parameter. The result depends on Hexasmal label quality
(checked in step 02), the declarative `laposte.root` exceptions and the
fuzzy matching threshold.

### Position in the process

```mermaid
flowchart LR
    E04[04 · Typology] --> E05["05 · Postal codes<br/>(this step)"] --> OUT[(Enriched pipeline<br/>outputs)]
```

## 2. Functional level

### Implementation

- Entry function: [`merge_postal_codes`](../../reference/processings.md)
  (`src/rnipp_geo_data_collector/processings/postal_codes_merge.py`)
- Key functions: `get_postal_codes_current_geo_entity`,
  `get_postal_codes_historical_geo_entity`,
  `assign_postal_codes_by_hexasmal_labels`,
  `propagate_postal_codes_in_component`,
  `assign_postal_codes_from_arrondissements`
- Graph: [`build_history_graph_of_geo_entities`](../../reference/graph.md)
  on `insee_communes`, `insee_arrondissements_municipaux`
- Matching: `normalize_location_label` / `is_label_location_similar`
  (rapidfuzz)

### Business rules

| Rule | Wording | Source |
|---|---|---|
| Current entities | Postal codes attached to the matched current entity (Hexasmal INSEE code + label) | `get_postal_codes_current_geo_entity` |
| Shared leaf code | If every leaf of a connected component has the same single postal code, it is propagated to the whole component | `get_postal_codes_historical_geo_entity` |
| Associated name | A Hexasmal entry with `associated_name` is attributed to the best-matching vertex (highest label similarity, closest to the source on ties) and dedicated to it | `assign_postal_codes_by_hexasmal_labels` |
| Delivery label | An entry without `associated_name` (or with one matching no vertex) goes to every vertex whose label matches `delivery_label`/`name` | `assign_postal_codes_by_hexasmal_labels` |
| Fusion (in-degree ≥ 2) | A merged entity inherits the union of its predecessors' postal codes, even if it already has some (fusion-défusion) | `propagate_postal_codes_in_component` |
| Unique successor | A vertex with no code and a single successor inherits its codes, except those dedicated to another vertex | `propagate_postal_codes_in_component` |
| Communes with arrondissements | A commune absent from Hexasmal (Paris, Lyon, Marseille) gets the union of its municipal arrondissements' codes | `assign_postal_codes_from_arrondissements` |
| Ambiguity | A label matching several entities without disambiguation is not attributed | guards in the matching |
| Count | `postal_codes_count` = cardinality of the attributed codes | final write |

### Inputs / Outputs / Dependencies

| | Detail |
|---|---|
| **Inputs** | Tables `insee_communes`, `insee_arrondissements_municipaux`, `laposte_hexasmal` (registry), DuckDB |
| **Outputs** | `<work>/05_processing/*.csv` with `postal_codes`, `postal_codes_count` columns |
| **Dependencies** | `graph/graph_event.py`, igraph, rapidfuzz |

### Detailed behaviour

```mermaid
sequenceDiagram
    participant DB as DuckDB
    participant G as Event graph
    participant H as Hexasmal (step 02)
    participant W as Final CSV
    DB->>G: communes + arrondissements
    H->>G: labels + postal codes
    G->>G: fuzzy label matching → INSEE entity
    alt matched current entity
        G->>W: attached postal codes
    else historical entity
        G->>G: propagation if unique path to a leaf
        G->>W: propagated postal codes
    end
    W->>W: postal_codes, postal_codes_count columns
```

## 3. Technical level

### Change the behaviour

- The disambiguation strategy lives in
  `assign_postal_codes_by_hexasmal_labels` and
  `get_postal_codes_historical_geo_entity`: any attribution rule change
  goes through them.
- The similarity threshold is shared with step 04 — if you change it,
  change it in both places and update both cards.
- Declarative exceptions (`laposte.root`) remain the preferred mechanism
  for a specific case: do not code a one-off fix.

### Test the change

```bash
poetry run ruff check . && poetry run pyright
poetry run geo_data_collector --working-directory /tmp/test-cp \
    --overwrite-working-directory
python -c "
import csv
rows = list(csv.DictReader(open('/tmp/test-cp/05_processing/communes.csv')))
matched = sum(bool(r['postal_codes']) for r in rows)
print(f'{matched}/{len(rows)} communes with a postal code')
"
```

### Update the documentation

1. "Business rules" table and sequence diagram of this card.
2. Docstrings (rendered in the
   [processings API](../../reference/processings.md)).
3. If the matching rate changes significantly, update
   [Methodology](../methodology.md) (design decisions).
