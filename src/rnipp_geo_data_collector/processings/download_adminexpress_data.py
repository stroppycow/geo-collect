import logging
from pathlib import Path

import duckdb

from ..acquisition.config import AcquisitionConfig, ErrorHandlerConfig
from ..acquisition.suppliers.adminexpress.requests import (
    OutputPathsRequestAdminExpressCog,
    RequestAdminExpressArrondissementMunicipal,
    RequestAdminExpressCollectiviteTerritoriale,
    RequestAdminExpressCommune,
    RequestAdminExpressCommuneAssocieeOuDeleguee,
    RequestAdminExpressDepartement,
)
from ..metadata import GeoParquetFileMetadata, InfoCurrentStoredData

logger = logging.getLogger(__name__)


def download_adminexpress_data(
    acquisition_config: AcquisitionConfig,
    exceptions_handler_config: ErrorHandlerConfig,
    duckdb_conn: duckdb.DuckDBPyConnection,
    output_dir: Path,
    current_stored_data: InfoCurrentStoredData,
) -> None:
    """Download the Admin Express COG contours (GeoParquet) of the IGN for the
    COG entities: collectivités territoriales, arrondissements municipaux,
    départements, communes and communes associées ou déléguées.

    The latest GeoParquet edition for the configured zone is resolved from the
    Géoplateforme download catalog; each layer is then downloaded, checked and
    registered in ``current_stored_data`` as a GeoParquet file.
    """
    output_dir_adminexpress = output_dir
    output_dir_adminexpress.mkdir(parents=True, exist_ok=True)
    output_dir_adminexpress_raw = output_dir_adminexpress / "raw"
    output_dir_adminexpress_raw.mkdir(parents=True, exist_ok=True)
    output_dir_adminexpress_cleaned = output_dir_adminexpress / "cleaned"
    output_dir_adminexpress_cleaned.mkdir(parents=True, exist_ok=True)
    output_dir_adminexpress_remove = output_dir_adminexpress / "remove"
    output_dir_adminexpress_remove.mkdir(parents=True, exist_ok=True)
    output_dir_adminexpress_add_or_replace = output_dir_adminexpress / "add_or_replace"
    output_dir_adminexpress_add_or_replace.mkdir(parents=True, exist_ok=True)

    layers = [
        (
            "collectivite_territoriale.parquet",
            RequestAdminExpressCollectiviteTerritoriale,
            exceptions_handler_config.adminexpress.collectivites_territoriales,
            "adminexpress_collectivites_territoriales",
        ),
        (
            "arrondissement_municipal.parquet",
            RequestAdminExpressArrondissementMunicipal,
            exceptions_handler_config.adminexpress.arrondissements_municipaux,
            "adminexpress_arrondissements_municipaux",
        ),
        (
            "departement.parquet",
            RequestAdminExpressDepartement,
            exceptions_handler_config.adminexpress.departements,
            "adminexpress_departements",
        ),
        (
            "commune.parquet",
            RequestAdminExpressCommune,
            exceptions_handler_config.adminexpress.communes,
            "adminexpress_communes",
        ),
        (
            "commune_associee_ou_deleguee.parquet",
            RequestAdminExpressCommuneAssocieeOuDeleguee,
            exceptions_handler_config.adminexpress.communes_associees_ou_deleguees,
            "adminexpress_communes_associees_ou_deleguees",
        ),
    ]

    for (
        filename,
        request_class,
        layer_exceptions_handler_config,
        registry_key,
    ) in layers:
        request = request_class(
            output_paths=OutputPathsRequestAdminExpressCog(
                raw_entities=output_dir_adminexpress_raw / filename,
                add_or_replace_entities=output_dir_adminexpress_add_or_replace
                / filename.replace(".parquet", ".csv"),
                remove_entities=output_dir_adminexpress_remove
                / filename.replace(".parquet", ".csv"),
                cleaned_entities=output_dir_adminexpress_cleaned / filename,
            ),
            acquisition_config=acquisition_config.adminexpress,
            exceptions_handler_config=layer_exceptions_handler_config,
        )
        logger.info(f"Downloading {request.description} from Admin Express COG")
        try:
            request.send()
        except Exception as e:
            logger.error(f"Error downloading {request.description} data: {e}")
            raise RuntimeError(
                f"Failed to download {request.description} data: {e}"
            ) from e
        try:
            request.check_content(duckdb_conn=duckdb_conn)
        except Exception as e:
            logger.error(f"Error checking content of {request.description} data: {e}")
            raise RuntimeError(
                f"Failed to check content of {request.description} data: {e}"
            ) from e
        current_stored_data.add_data(
            key=registry_key,
            value=GeoParquetFileMetadata(
                path=request.output_paths.cleaned_entities,
                colnames=request.colnames,
            ),
        )
