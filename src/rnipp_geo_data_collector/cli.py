from typing import Optional
import typer
from .main import collect_geo_data

app = typer.Typer()

@app.command()
def cmd_collect_geo_data(
    acquisition_config_file: Optional[str] = typer.Option(None, help="Path to the acquisition configuration file"),
    exceptions_handler_config_file: Optional[str] = typer.Option(None, help="Path to the exceptions handler configuration file"),
    working_directory: Optional[str] = typer.Option(None, help="Working directory to store data and temporary files"),
    overwrite_working_directory: bool = typer.Option(False, help="Allow replacing the working directory if it already exists"),
    threads: int = typer.Option(1, help="Number of threads to use"),
    duckdb_extension_directory: Optional[str] = typer.Option(None, help="Directory for DuckDB extensions"),
    duckdb_memory_limit: str = typer.Option("10GB", help="Total memory limit for DuckDB"),
    duckdb_max_temp_directory_size: str = typer.Option("50GB", help="Maximum size for DuckDB temporary directory"),
    loglevel: str = typer.Option("INFO", help="Logging level")
    ):
    collect_geo_data(
        acquisition_config_file=acquisition_config_file,
        exceptions_handler_config_file=exceptions_handler_config_file,
        working_directory=working_directory,
        overwrite_working_directory=overwrite_working_directory,
        threads=threads,
        duckdb_extension_directory=duckdb_extension_directory,
        duckdb_memory_limit=duckdb_memory_limit,
        duckdb_max_temp_directory_size=duckdb_max_temp_directory_size,
        loglevel=loglevel
    )

if __name__ == "__main__":
    app()