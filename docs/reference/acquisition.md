# Acquisition API

Module `rnipp_geo_data_collector.acquisition`: configuration loading,
supplier requests and quality checks.

## Configuration

::: rnipp_geo_data_collector.acquisition.config

## INSEE supplier

Requests per entity type:

::: rnipp_geo_data_collector.acquisition.suppliers.insee.requests

Checks (one check = one class, failing loudly):

::: rnipp_geo_data_collector.acquisition.suppliers.insee.checks.abstract

## LaPoste supplier

::: rnipp_geo_data_collector.acquisition.suppliers.laposte.requests

::: rnipp_geo_data_collector.acquisition.suppliers.laposte.checks.abstract

## IGN Admin Express supplier

Requests per Admin Express COG layer (GeoParquet, resolved from the
Géoplateforme download catalog):

::: rnipp_geo_data_collector.acquisition.suppliers.adminexpress.requests

Checks (one check = one class, failing loudly):

::: rnipp_geo_data_collector.acquisition.suppliers.adminexpress.checks.abstract

Check details and their business rules are described in the step cards:
[01 — INSEE COG](../explanation/steps/01-download-insee-cog.md),
[02 — LaPoste Hexasmal](../explanation/steps/02-download-laposte.md),
[06 — Admin Express COG](../explanation/steps/06-download-adminexpress-cog.md).
