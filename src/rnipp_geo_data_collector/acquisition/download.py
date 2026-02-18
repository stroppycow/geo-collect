import logging
from pathlib import Path
import duckdb

from .config import AcquisitionConfig, ErrorHandlerConfig
from .suppliers.insee.requests import RequestCOGArrondissementMunicipal, RequestCOGCommune, RequestsCOGCollectivitesOutremer, RequestsCOGDistrict, RequestsCOGEvents, RequestsCOGPays
from .suppliers.laposte import RequestLaPosteHexasmal


def download_geo_data(
    acquisition_config: AcquisitionConfig,
    exceptions_handler_config: ErrorHandlerConfig,
    duckdb_conn : duckdb.DuckDBPyConnection,
    output_dir: Path
):
    """Download data from supplied URLs."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_dir_insee = output_dir / "insee"
    output_dir_insee.mkdir(parents=True, exist_ok=True)
    output_dir_insee_raw = output_dir_insee / "raw"
    output_dir_insee_raw.mkdir(parents=True, exist_ok=True)
    output_dir_insee_cleaned = output_dir_insee / "cleaned"
    output_dir_insee_cleaned.mkdir(parents=True, exist_ok=True)

    request_insee_commune = RequestCOGCommune(
        output_path_before_checks = output_dir_insee_raw / "communes.csv",
        output_path_after_checks = output_dir_insee_cleaned / "communes.csv",
        acquisition_config = acquisition_config.insee,
        exceptions_handler_config = exceptions_handler_config.insee.communes
    )
    request_insee_arrondissement_municipal = RequestCOGArrondissementMunicipal(
            output_path_before_checks = output_dir_insee_raw / "arrondissements_municipaux.csv",
            output_path_after_checks = output_dir_insee_cleaned / "arrondissements_municipaux.csv",
            acquisition_config = acquisition_config.insee,
            exceptions_handler_config = exceptions_handler_config.insee.arrondissements_municipaux
    )
    request_insee_collectivites_outremer = RequestsCOGCollectivitesOutremer(
        output_path_before_checks = output_dir_insee_raw / "collectivites_outremer.csv",
        output_path_after_checks = output_dir_insee_cleaned / "collectivites_outremer.csv",
        acquisition_config = acquisition_config.insee,
        exceptions_handler_config = exceptions_handler_config.insee.collectivites_outremer
    )
    request_insee_districts = RequestsCOGDistrict(
        output_path_before_checks = output_dir_insee_raw / "districts.csv",
        output_path_after_checks = output_dir_insee_cleaned / "districts.csv",
        acquisition_config = acquisition_config.insee,
        exceptions_handler_config = exceptions_handler_config.insee.districts
    )
    request_insee_pays = RequestsCOGPays(
        output_path_before_checks = output_dir_insee_raw / "pays.csv",
        output_path_after_checks = output_dir_insee_cleaned / "pays.csv",
        acquisition_config = acquisition_config.insee,
        exceptions_handler_config = exceptions_handler_config.insee.pays
    )
    request_insee_events = RequestsCOGEvents(
        output_path_before_checks = output_dir_insee_raw / "events.csv",
        output_path_after_checks = output_dir_insee_cleaned / "events.csv",
        acquisition_config = acquisition_config.insee,
        exceptions_handler_config = exceptions_handler_config.insee.events
    )

    logging.info(f"Downloading \"Communes\" data from COG")
    try:
        request_insee_commune.send()
    except Exception as e:
        logging.error(f"Error downloading \"Communes\" data: {e}")
        raise RuntimeError(f"Failed to download \"Communes\" data: {e}") from e
    try:
        request_insee_commune.check_content(duckdb_conn = duckdb_conn)
    except Exception as e:
        logging.error(f"Error checking content of \"Communes\" data: {e}")
        raise RuntimeError(f"Failed to check content of \"Communes\" data: {e}") from e


    logging.info(f"Downloading \"Arrondissements Municipaux\" data from COG")
    try:
        request_insee_arrondissement_municipal.send()
    except Exception as e:
        logging.error(f"Error downloading \"Arrondissements Municipaux\" data: {e}")
        raise RuntimeError(f"Failed to download \"Arrondissements Municipaux\" data: {e}") from e
    try:
        request_insee_arrondissement_municipal.check_content(duckdb_conn = duckdb_conn)
    except Exception as e:
        logging.error(f"Error checking content of \"Arrondissements Municipaux\" data: {e}")
        raise RuntimeError(f"Failed to check content of \"Arrondissements Municipaux\" data: {e}") from e

    
    logging.info(f"Downloading \"Collectivités d'Outre-mer\" data from COG")
    try:
        request_insee_collectivites_outremer.send()
    except Exception as e:
        logging.error(f"Error downloading \"Collectivités d'Outre-mer\" data: {e}")
        raise RuntimeError(f"Failed to download \"Collectivités d'Outre-mer\" data: {e}") from e
    try:
        request_insee_collectivites_outremer.check_content(duckdb_conn = duckdb_conn)
    except Exception as e:
        logging.error(f"Error checking content of \"Collectivités d'Outre-mer\" data: {e}")
        raise RuntimeError(f"Failed to check content of \"Collectivités d'Outre-mer\" data: {e}") from e
    
    logging.info(f"Downloading \"Districts\" data from COG")
    try:
        request_insee_districts.send()
    except Exception as e:
        logging.error(f"Error downloading \"Districts\" data: {e}")
        raise RuntimeError(f"Failed to download \"Districts\" data: {e}") from e
    try:
        request_insee_districts.check_content(duckdb_conn = duckdb_conn)
    except Exception as e:
        logging.error(f"Error checking content of \"Districts\" data: {e}")
        raise RuntimeError(f"Failed to check content of \"Districts\" data: {e}") from e
    
    logging.info(f"Downloading \"Pays\" data from COG")
    try:
        request_insee_pays.send()
    except Exception as e:
        logging.error(f"Error downloading \"Pays\" data: {e}")
        raise RuntimeError(f"Failed to download \"Pays\" data: {e}") from e
    try:
        request_insee_pays.check_content(duckdb_conn = duckdb_conn)
    except Exception as e:
        logging.error(f"Error checking content of \"Pays\" data: {e}")
        raise RuntimeError(f"Failed to check content of \"Pays\" data: {e}") from e
    try:
        request_insee_events.send()
    except Exception as e:
        logging.error(f"Error downloading \"Events\" data: {e}")
        raise RuntimeError(f"Failed to download \"Events\" data: {e}") from e
    try:
        request_insee_events.check_content(duckdb_conn = duckdb_conn)
    except Exception as e:
        logging.error(f"Error checking content of \"Events\" data: {e}")
        raise RuntimeError(f"Failed to check content of \"Events\" data: {e}") from e

    try:
        RequestLaPosteHexasmal(
            output_path = output_dir / "laposte" / "hexasmal.csv",
            endpoint_url = acquisition_config.laposte.endpoint_url,
            backoff_factor = acquisition_config.laposte.backoff_factor,
            max_retries = acquisition_config.laposte.max_retries,
            timeout = (acquisition_config.laposte.connect_timeout, acquisition_config.laposte.read_timeout)
        ).send()
    except Exception as e:
        logging.error(f"Error downloading \"Hexasmal\" data", exc_info=True)
