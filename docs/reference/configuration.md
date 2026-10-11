# Configuration

All configuration goes through pydantic models: provided YAML files are
**validated** at load time; a wrongly typed value stops the run with an
explicit message.

## Acquisition

::: rnipp_geo_data_collector.acquisition.config.AcquisitionConfig
    options:
      show_root_heading: true

::: rnipp_geo_data_collector.acquisition.suppliers.insee.config.InseeSupplierConfig
    options:
      show_root_heading: true

::: rnipp_geo_data_collector.acquisition.suppliers.laposte.config.LaPosteSupplierConfig
    options:
      show_root_heading: true

## Exception handling

::: rnipp_geo_data_collector.acquisition.config.ErrorHandlerConfig
    options:
      show_root_heading: true

Two families of objects in the `root` lists:

- `AddOrReplace` (per entity type: `InseeCommuneAddOrReplace`,
  `InseeDepartementAddOrReplace`...) — corrects an entity;
- `InseeGeoRemove` — ignores an entity.

Declared URIs must be unique (`check_unique_uri` validator).

## File loading

`AcquisitionConfig.from_file()` / `ErrorHandlerConfig.from_file()`
accept a YAML or JSON path and raise explicit errors:

- `FileNotFoundError` — missing file;
- `ValueError` — unsupported format, invalid YAML/JSON syntax, or values
  rejected by the pydantic validators.

## Environment variables

The `AcquisitionConfig` model enables environment-based settings: prefix
`GEOCOLLECT_`, nested separator `__`.
Example: `GEOCOLLECT_INSEE__MAX_RETRIES=3`.
