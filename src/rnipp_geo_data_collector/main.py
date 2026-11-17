import logging
import shutil
from pathlib import Path

import duckdb

from .acquisition.config import AcquisitionConfig, ErrorHandlerConfig
from .metadata import InfoCurrentStoredData
from .processings.communes_typology import typologize_communes
from .processings.download_adminexpress_data import download_adminexpress_data
from .processings.download_cog_data import download_cog_data
from .processings.download_laposte_data import download_laposte_data
from .processings.metropolitan_france_dummy_correction import (
    correct_metropolitan_france_dummy,
)
from .processings.postal_codes_merge import merge_postal_codes
from .utils.duckdb import init_duckdb_connection
from .utils.logging import configure_logging

logger = logging.getLogger(__name__)


def _close_duckdb_connection(duckdb_connection: duckdb.DuckDBPyConnection) -> None:
    """Close the DuckDB connection, logging any error instead of raising it.

    Used on error-handling paths so that a failure while closing the
    connection never masks the original exception.
    """
    try:
        duckdb_connection.close()
    except duckdb.Error as e:
        logger.error(f"Failed to close DuckDB connection: {e}")


_KNOWN_OFFLINE_SOURCES = ("insee", "laposte", "adminexpress")


def _parse_offline_sources(offline: bool, offline_sources: str | None) -> set[str]:
    """Resolve which acquisition sources are read offline.

    `--offline` means all sources; `--offline-sources` selects a
    comma-separated subset of them (e.g. "adminexpress" or "insee,laposte").
    """
    if offline and offline_sources is not None:
        raise ValueError("--offline and --offline-sources are mutually exclusive")
    if offline:
        return set(_KNOWN_OFFLINE_SOURCES)
    if offline_sources is None:
        return set()
    sources = {s.strip().lower() for s in offline_sources.split(",") if s.strip()}
    unknown = sources - set(_KNOWN_OFFLINE_SOURCES)
    if unknown:
        raise ValueError(
            f"Unknown sources in --offline-sources: {sorted(unknown)}. "
            f"Expected a comma-separated subset of: {', '.join(_KNOWN_OFFLINE_SOURCES)}"
        )
    return sources


def collect_geo_data(
    acquisition_config_file: None | str | Path = None,
    exceptions_handler_config_file: None | str | Path = None,
    working_directory: None | str | Path = None,
    overwrite_working_directory: bool = False,
    offline: bool = False,
    offline_data_directory: None | str | Path = None,
    offline_sources: str | None = None,
    threads: int = 1,
    duckdb_extension_directory: str | None = None,
    duckdb_memory_limit: str = "10GB",
    duckdb_max_temp_directory_size: str = "50GB",
    loglevel: str = "INFO",
    log_stdout: bool = True,
    log_file: None | str | Path = None,
):

    # Configure logging
    configure_logging(loglevel=loglevel, log_stdout=log_stdout, log_file=log_file)

    # Set up offline mode (per source: raw files are provided instead of downloaded)
    active_offline_sources = _parse_offline_sources(
        offline=offline, offline_sources=offline_sources
    )
    offline_data_directory_path: Path | None = None
    if active_offline_sources and offline_data_directory is None:
        raise ValueError(
            "Offline mode (--offline or --offline-sources) requires "
            "--offline-data-directory with the pre-downloaded raw files"
        )
    if offline_data_directory is not None and not active_offline_sources:
        raise ValueError(
            "--offline-data-directory requires --offline or --offline-sources"
        )
    if offline_data_directory is not None:
        if isinstance(offline_data_directory, str):
            offline_data_directory_path = Path(offline_data_directory)
        else:
            offline_data_directory_path = offline_data_directory
        if not offline_data_directory_path.is_dir():
            raise ValueError(
                f"Offline data directory {offline_data_directory_path} does not exist"
            )
        offline_data_directory_path = offline_data_directory_path.resolve()
        logger.info(
            f"Offline sources {sorted(active_offline_sources)}: raw files are read "
            f"from {offline_data_directory_path} "
            "(<dir>/insee/, <dir>/laposte/, <dir>/adminexpress/); no download will "
            "be attempted for these sources"
        )

    # Set up working directory
    logger.info("Setting up working directory")
    working_directory_path = Path(".")
    if working_directory is None:
        working_directory_path = Path.cwd() / "output"
    elif isinstance(working_directory, str):
        working_directory_path = Path(working_directory)
    else:
        working_directory_path = working_directory
    logger.info(f"Working directory: {working_directory_path}")

    if working_directory_path.exists():
        if not overwrite_working_directory:
            raise RuntimeError(
                f"Working directory {working_directory_path} already exists."
            )
        try:
            shutil.rmtree(working_directory_path)
        except Exception as e:
            raise RuntimeError(
                f"Failed to remove working directory {working_directory_path}"
            ) from e

    working_directory_path.mkdir(parents=True, exist_ok=True)

    # Set up DuckDB
    duckdb_connection = init_duckdb_connection(
        extension_directory=duckdb_extension_directory,
        threads=threads,
        memory_limit=duckdb_memory_limit,
        max_temp_directory_size=duckdb_max_temp_directory_size,
        temp_directory_duckdb=working_directory_path,
    )

    # Set up acquisition config
    if acquisition_config_file is None:
        acquisition_config = AcquisitionConfig()
    else:
        logger.info(f"Loading acquisition config from {acquisition_config_file}")
        acquisition_config = AcquisitionConfig.from_file(acquisition_config_file)

    if exceptions_handler_config_file is None:
        exceptions_handler_config = ErrorHandlerConfig()
    else:
        logger.info(
            f"Loading exceptions handler config from {exceptions_handler_config_file}"
        )
        exceptions_handler_config = ErrorHandlerConfig.from_file(
            exceptions_handler_config_file
        )

    current_stored_data = InfoCurrentStoredData()

    def _offline_raw_dir(source: str) -> Path | None:
        """Raw directory of an offline source: ``<offline-data-directory>/<source>/``
        (e.g. ``<dir>/adminexpress/``), named after the source — not after the
        processing step order."""
        if offline_data_directory_path is None or source not in active_offline_sources:
            return None
        return offline_data_directory_path / source

    # Download COG data
    try:
        download_cog_data(
            acquisition_config=acquisition_config,
            exceptions_handler_config=exceptions_handler_config,
            duckdb_conn=duckdb_connection,
            output_dir=working_directory_path / "01_processing",
            current_stored_data=current_stored_data,
            offline_raw_dir=_offline_raw_dir("insee"),
        )
    except Exception as e:
        _close_duckdb_connection(duckdb_connection)
        logger.error(f"Failed to download COG data: {e}")
        raise RuntimeError(f"Failed to download COG data: {e}") from e

    try:
        download_laposte_data(
            acquisition_config=acquisition_config,
            exceptions_handler_config=exceptions_handler_config,
            duckdb_conn=duckdb_connection,
            output_dir=working_directory_path / "02_processing",
            current_stored_data=current_stored_data,
            offline_raw_dir=_offline_raw_dir("laposte"),
        )
    except Exception as e:
        _close_duckdb_connection(duckdb_connection)
        logger.error(f"Failed to download LaPoste data: {e}")
        raise RuntimeError(f"Failed to download LaPoste data: {e}") from e

    try:
        correct_metropolitan_france_dummy(
            duckdb_connection=duckdb_connection,
            current_stored_data=current_stored_data,
            output_dir=working_directory_path / "03_processing",
        )
    except Exception as e:
        _close_duckdb_connection(duckdb_connection)
        logger.error("Failed to correct Metropolitan France dummy variable")
        raise RuntimeError(
            f"Failed to correct Metropolitan France dummy variable: {e}"
        ) from e

    try:
        typologize_communes(
            duckdb_connection=duckdb_connection,
            current_stored_data=current_stored_data,
            output_dir=working_directory_path / "04_processing",
        )
    except Exception as e:
        _close_duckdb_connection(duckdb_connection)
        logger.error("Failed to typologize communes location")
        raise RuntimeError(f"Failed to typologize communes location: {e}") from e

    try:
        merge_postal_codes(
            duckdb_connection=duckdb_connection,
            current_stored_data=current_stored_data,
            output_dir=working_directory_path / "05_processing",
        )
    except Exception as e:
        _close_duckdb_connection(duckdb_connection)
        logger.error("Failed to merge postal codes")
        raise RuntimeError(f"Failed to merge postal codes: {e}") from e

    try:
        download_adminexpress_data(
            acquisition_config=acquisition_config,
            exceptions_handler_config=exceptions_handler_config,
            duckdb_conn=duckdb_connection,
            output_dir=working_directory_path / "06_processing",
            current_stored_data=current_stored_data,
            offline_raw_dir=_offline_raw_dir("adminexpress"),
        )
    except Exception as e:
        _close_duckdb_connection(duckdb_connection)
        logger.error("Failed to download Admin Express COG data")
        raise RuntimeError(f"Failed to download Admin Express COG data: {e}") from e

    try:
        duckdb_connection.close()
    except duckdb.Error as e:
        logger.error(f"Failed to close DuckDB connection: {e}")
        raise RuntimeError(f"Failed to close DuckDB connection: {e}") from e
