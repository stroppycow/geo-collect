import logging
from pathlib import Path

import duckdb

from ..acquisition.config import AcquisitionConfig, ErrorHandlerConfig
from ..acquisition.suppliers.laposte.requests import (
    OutputPathsRequestLaPosteHexasmal,
    RequestLaPosteHexasmal,
)
from ..metadata import GeoCSVFileMetadata, InfoCurrentStoredData

logger = logging.getLogger(__name__)


def download_laposte_data(
    acquisition_config: AcquisitionConfig,
    exceptions_handler_config: ErrorHandlerConfig,
    duckdb_conn: duckdb.DuckDBPyConnection,
    output_dir: Path,
    current_stored_data: InfoCurrentStoredData,
) -> None:
    """Download data from supplied URLs."""
    output_dir_laposte = output_dir
    output_dir_laposte.mkdir(parents=True, exist_ok=True)
    output_dir_laposte_raw = output_dir_laposte / "raw"
    output_dir_laposte_raw.mkdir(parents=True, exist_ok=True)
    output_dir_laposte_cleaned = output_dir_laposte / "cleaned"
    output_dir_laposte_cleaned.mkdir(parents=True, exist_ok=True)
    output_dir_laposte_remove = output_dir_laposte / "remove"
    output_dir_laposte_remove.mkdir(parents=True, exist_ok=True)
    output_dir_laposte_add = output_dir_laposte / "add"
    output_dir_laposte_add.mkdir(parents=True, exist_ok=True)

    filenames_laposte = "laposte_hexasmal.csv"
    request_laposte_hexasmal = RequestLaPosteHexasmal(
        output_paths=OutputPathsRequestLaPosteHexasmal(
            raw_entities=output_dir_laposte_raw / filenames_laposte,
            add_entities=output_dir_laposte_add / filenames_laposte,
            remove_entities=output_dir_laposte_remove / filenames_laposte,
            cleaned_entities=output_dir_laposte_cleaned / filenames_laposte,
        ),
        exceptions_handler_config=exceptions_handler_config.laposte,
        acquisition_config=acquisition_config.laposte,
    )
    logger.info('Downloading "La Poste Hexasmal" data')
    try:
        request_laposte_hexasmal.send()
    except Exception as e:
        logger.error(f'Error downloading "La Poste Hexasmal" data: {e}')
        raise RuntimeError(f'Failed to download "La Poste Hexasmal" data: {e}') from e
    try:
        request_laposte_hexasmal.check_content(duckdb_conn=duckdb_conn)
    except Exception as e:
        logger.error(f'Error checking content of "La Poste Hexasmal" data: {e}')
        raise RuntimeError(
            f'Failed to check content of "La Poste Hexasmal" data: {e}'
        ) from e
    current_stored_data.add_data(
        key="laposte_hexasmal",
        value=GeoCSVFileMetadata(
            path=request_laposte_hexasmal.output_paths.cleaned_entities,
            header=True,
            delim=",",
            encoding="utf-8",
            colnames=request_laposte_hexasmal.colnames,
        ),
    )
