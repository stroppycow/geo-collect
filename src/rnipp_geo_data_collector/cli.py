import typer

from .main import collect_geo_data

app = typer.Typer()


@app.command()
def cmd_collect_geo_data(
    acquisition_config_file: str | None = typer.Option(
        None, help="Path to the acquisition configuration file"
    ),
    exceptions_handler_config_file: str | None = typer.Option(
        None, help="Path to the exceptions handler configuration file"
    ),
    working_directory: str | None = typer.Option(
        None, help="Working directory to store data and temporary files"
    ),
    overwrite_working_directory: bool = typer.Option(
        False, help="Allow replacing the working directory if it already exists"
    ),
    threads: int = typer.Option(1, help="Number of threads to use"),
    duckdb_extension_directory: str | None = typer.Option(
        None, help="Directory for DuckDB extensions"
    ),
    duckdb_memory_limit: str = typer.Option(
        "10GB", help="Total memory limit for DuckDB"
    ),
    duckdb_max_temp_directory_size: str = typer.Option(
        "50GB", help="Maximum size for DuckDB temporary directory"
    ),
    loglevel: str = typer.Option("INFO", help="Logging level"),
    log_stdout: bool = typer.Option(
        True,
        "--log-stdout/--no-log-stdout",
        help="Emit log messages to stdout",
    ),
    log_file: str | None = typer.Option(
        None, help="Path to a log file; if set, log messages are also written to this file"
    ),
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
        loglevel=loglevel,
        log_stdout=log_stdout,
        log_file=log_file,
    )


if __name__ == "__main__":
    app()
