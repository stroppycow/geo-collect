from typing import Optional, Union
from pathlib import Path
import shutil
import logging


from .acquisition.config import AcquisitionConfig, ErrorHandlerConfig
from .acquisition.download import download_geo_data
from .processings.metropolitan_france_dummy_correction import correct_metropolitan_france_dummy
from .processings.communes_typology import typologize_communes
from .processings.postal_codes_merge import merge_postal_codes

from .utils.duckdb import init_duckdb_connection
from .metadata import InfoCurrentStoredData


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

    current_stored_data = InfoCurrentStoredData()
    # Download geo data
    try:
        download_geo_data(
            acquisition_config = acquisition_config,
            exceptions_handler_config = exceptions_handler_config,
            duckdb_conn = duckdb_connection,
            output_dir = working_directory_path / 'download',
            current_stored_data = current_stored_data
        )
    except Exception as e:
        duckdb_connection.close()
        logging.error(f"Failed to download geo data: {e}")
        raise RuntimeError(f"Failed to download geo data: {e}") from e
    
    try:
        correct_metropolitan_france_dummy(
            duckdb_connection=duckdb_connection,
            current_stored_data=current_stored_data,
            output_dir=working_directory_path / '01_processing'
        )
    except Exception as e:
        duckdb_connection.close()
        logging.error(f"Failed to correct Metropolitan France dummy variable")
        raise RuntimeError(f"Failed to correct Metropolitan France dummy variable: {e}") from e
    
    try:
        typologize_communes(
            duckdb_connection=duckdb_connection,
            current_stored_data=current_stored_data,
            output_dir=working_directory_path / '02_processing'
        )
    except Exception as e:
        duckdb_connection.close()
        logging.error(f"Failed to typologize communes location")
        raise RuntimeError(f"Failed to typologize communes location: {e}") from e
    
    try:
        merge_postal_codes(
            duckdb_connection=duckdb_connection,
            current_stored_data=current_stored_data,
            output_dir=working_directory_path / '03_processing'
        )
    except Exception as e:
        duckdb_connection.close()
        logging.error(f"Failed to merge postal codes")
        raise RuntimeError(f"Failed to merge postal codes: {e}") from e
    
    try:
        duckdb_connection.close()
    except Exception as e:
        logging.error(f"Failed to close DuckDB connection: {e}")
