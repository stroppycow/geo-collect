# Data model

The `InfoCurrentStoredData` registry acts as a catalogue: every loaded file
is registered with its path and its typed columns. Pipeline steps read
**only** this registry.

::: rnipp_geo_data_collector.metadata.InfoCurrentStoredData
    options:
      show_root_heading: true

::: rnipp_geo_data_collector.metadata.GeoCSVFileMetadata
    options:
      show_root_heading: true

## Column typing

Every column carries a `ColumnDataType` that holds both the DuckDB type
and the SQL parsing expression:

| Class | DuckDB type | Usage |
|---|---|---|
| `StringColumnDataType` | `VARCHAR` | URIs, labels |
| `StringListColumnDataType` | `VARCHAR` | Separated lists (e.g. `parent_uri`) |
| `BooleanColumnDataType` | `BOOLEAN` | Flags (e.g. `is_france_metropolitaine`) |
| `DateColumnDataType` | `DATE` | Validity dates |

::: rnipp_geo_data_collector.metadata.ColumnDataType
    options:
      show_root_heading: true
