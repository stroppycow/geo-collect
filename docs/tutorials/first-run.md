# First geographic data collection

This tutorial runs a complete pipeline, from installation to inspecting the
results. Duration: about 15 minutes.

## 1. Prerequisites

- Python >= 3.11
- Network access to the INSEE and LaPoste endpoints

## 2. Installation

```bash
git clone https://github.com/stroppycow/geo-collect.git
cd geo-collect
poetry install
```

## 3. Run the collection

From the project root:

```bash
poetry run geo_data_collector \
    --acquisition-config-file "./config/config-acquisition.yaml" \
    --exceptions-handler-config-file "./config/config-exceptions-handler.yaml" \
    --working-directory "./output" \
    --overwrite-working-directory
```

The terminal shows the progress of the five pipeline steps
(default log level: `INFO`).

## 4. Check the results

The working directory is organised by step:

```text
output/
├── 01_processing/    # INSEE COG: raw/, cleaned/, remove/, add_or_replace/
├── 02_processing/    # LaPoste Hexasmal
├── 03_processing/    # Corrected departements (metropolitan France)
├── 04_processing/    # Typed communes (location_typology column)
└── 05_processing/    # Communes + postal codes (postal_codes* columns)
```

Open for instance `01_processing/cleaned/communes.csv`: each row is a dated
geographic entity (URI, label, start/end dates, parent links).

## 5. Understand what just happened

- Steps and their rationale: [Process methodology](../explanation/methodology.md)
- Details of a specific step: the [step cards](../explanation/steps/step-card-template.md)
- Role of each CLI option: [CLI reference](../reference/cli.md)

## 6. Next steps

- Tune sources and timeouts: [Configure acquisition](../how-to/configure-acquisition.md)
- Work around a known COG anomaly: [Handle exceptions](../how-to/handle-exceptions.md)
- Speed up a large run: [Tune performance](../how-to/tune-performance.md)
