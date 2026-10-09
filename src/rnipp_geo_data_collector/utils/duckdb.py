import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


def init_duckdb_connection(
    extension_directory: Path | str | None = None,
    threads: int = 1,
    memory_limit: str = "4GB",
    max_temp_directory_size: str = "10GB",
    temp_directory_duckdb: None | Path | str = None,
) -> duckdb.DuckDBPyConnection:
    """
    Init a DuckDB connection with the required extensions and configurations.
    """

    config: dict[str, str] = {
        "max_temp_directory_size": max_temp_directory_size,
        "threads": str(threads),
        "memory_limit": memory_limit,
    }
    if isinstance(temp_directory_duckdb, str):
        config["temp_directory"] = temp_directory_duckdb
    elif isinstance(temp_directory_duckdb, Path):
        config["temp_directory"] = str(temp_directory_duckdb.resolve())

    if extension_directory:
        if isinstance(extension_directory, Path):
            config["extension_directory"] = str(extension_directory.resolve())
        else:
            config["extension_directory"] = extension_directory

    try:
        logger.info("Init a DuckDB connection")
        con = duckdb.connect(database=":memory:", read_only=False, config=config)
    except Exception as e:
        logger.error(f"Error while initializing DuckDB connection : {e}")
        raise RuntimeError(f"Error while initializing DuckDB connection : {e}") from e

    try:
        logger.info("Loading ICU extension for DuckDB")
        con.load_extension("icu")
    except Exception as e:
        logger.error(f"Unable to load ICU extension in DuckDB : {e}")
        raise RuntimeError(f"Unable to load ICU extension in DuckDB : {e}") from e

    try:
        logger.info("Loading JSON extension for DuckDB")
        con.load_extension("json")
    except Exception as e:
        logger.warning(f"Unable to load JSON extension in DuckDB : {e}")
        raise RuntimeError(f"Unable to load JSON extension in DuckDB : {e}") from e

    return con
