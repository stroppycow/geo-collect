from typing import Optional, Union
from pathlib import Path
import shutil
import logging


from .acquisition.config import AcquisitionConfig, ErrorHandlerConfig
from .acquisition.download import download_geo_data
from .utils.duckdb import init_duckdb_connection

def collect_geo_data(
    acquisition_config_file: Union[None, str, Path] = None,
    exceptions_handler_config_file: Union[None, str, Path] = None,
    working_directory: Union[None, str, Path] = None,
    overwrite_working_directory: bool = False,
    threads: int = 1,
    duckdb_extension_directory: Optional[str] = None,
    duckdb_memory_limit: str = "10GB",
    duckdb_max_temp_directory_size: str = "50GB",
    loglevel: str = "INFO" 
):
    
    # Configure logging level
    logging.basicConfig(level=loglevel.upper())

    # Set up working directory
    logging.info("Setting up working directory")
    working_directory_path = Path('.')
    if working_directory is None:
        working_directory_path =  Path.cwd() / "output"
    elif isinstance(working_directory, str):
        working_directory_path = Path(working_directory)
    else:
        working_directory_path = working_directory
    logging.info(f"Working directory: {working_directory_path}")

    if working_directory_path.exists():
        if not overwrite_working_directory:
            raise RuntimeError(f"Working directory {working_directory_path} already exists.")
        try:
            shutil.rmtree(working_directory_path)
        except Exception as e:
            raise RuntimeError(f"Failed to remove working directory {working_directory_path}") from e
        
    working_directory_path.mkdir(parents=True, exist_ok=True)

    # Set up DuckDB
    duckdb_connection = init_duckdb_connection(
        extension_directory=duckdb_extension_directory,
        threads=threads,
        memory_limit=duckdb_memory_limit,
        max_temp_directory_size=duckdb_max_temp_directory_size,
        temp_directory_duckdb= working_directory_path
    )

    # Set up acquisition config
    if acquisition_config_file is None:
        acquisition_config = AcquisitionConfig()
    else:
        logging.info(f"Loading acquisition config from {acquisition_config_file}")
        acquisition_config = AcquisitionConfig.from_file(acquisition_config_file)

    if exceptions_handler_config_file is None:
        exceptions_handler_config = ErrorHandlerConfig()
    else:
        logging.info(f"Loading exceptions handler config from {exceptions_handler_config_file}")
        exceptions_handler_config = ErrorHandlerConfig.from_file(exceptions_handler_config_file)

    # Download geo data
    try:
        download_geo_data(
            acquisition_config = acquisition_config,
            exceptions_handler_config = exceptions_handler_config,
            duckdb_conn = duckdb_connection,
            output_dir = working_directory_path / 'download'
        )
    except Exception as e:
        duckdb_connection.close()
        logging.error(f"Failed to download geo data: {e}")
        raise RuntimeError(f"Failed to download geo data: {e}") from e

    try:
        duckdb_connection.close()
    except Exception as e:
        logging.error(f"Failed to close DuckDB connection: {e}")
