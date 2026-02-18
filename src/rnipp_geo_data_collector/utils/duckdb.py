import duckdb
from pathlib import Path
from typing import Optional, Union
import logging

def init_duckdb_connection(
        extension_directory: Optional[Union[Path, str]] = None,
        threads: int = 1,
        memory_limit: str = "4GB",
        max_temp_directory_size: str = "10GB",
        temp_directory_duckdb: Union[None, Path, str] = None,
    ) -> duckdb.DuckDBPyConnection:
    """
    Init a DuckDB connection with the required extensions and configurations.
    """

    config: dict[str, str] = {
                'max_temp_directory_size': max_temp_directory_size,
                "threads": str(threads),
                "memory_limit": memory_limit
    }
    if isinstance(temp_directory_duckdb, str):
        config["temp_directory"] = temp_directory_duckdb
    elif isinstance(temp_directory_duckdb, Path):
        config["temp_directory"] = str(temp_directory_duckdb.resolve())

    if extension_directory:
        if isinstance(extension_directory, Path):
            config["extension_directory"] = str(extension_directory.resolve())
        else:
            config["extension_directory"]  = extension_directory


    try:
        logging.info("Init a DuckDB connection")
        con = duckdb.connect(database=':memory:', read_only=False, config=config)
    except Exception as e:
        logging.error(f"Error while initializing DuckDB connection : {e}")
        raise RuntimeError(f"Error while initializing DuckDB connection : {e}") from e
    
    try:
        logging.info("Loading ICU extension for DuckDB")
        con.load_extension('icu')
    except Exception as e:
        logging.error(f"Unable to load ICU extension in DuckDB : {e}")
        raise RuntimeError(f"Unable to load ICU extension in DuckDB : {e}") from e

    try:
        logging.info("Loading JSON extension for DuckDB")
        con.load_extension('json')
    except Exception as e:
        logging.warning(f"Unable to load JSON extension in DuckDB : {e}")
        raise RuntimeError(f"Unable to load JSON extension in DuckDB : {e}") from e
        
    return con
