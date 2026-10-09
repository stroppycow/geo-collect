import logging
from pathlib import Path

import duckdb

from ..acquisition.config import AcquisitionConfig, ErrorHandlerConfig
from ..acquisition.suppliers.insee.checks.events_consistency import (
    CheckEventsConsistencyAfterDownloadInseeCog,
)
from ..acquisition.suppliers.insee.checks.insee_code_overlap import (
    CheckGlobalInseeCodeOverlapAfterDownloadInseeCog,
)
from ..acquisition.suppliers.insee.checks.parent_period_include import (
    CheckParentPeriodsContainChildPeriodAfterDownloadInseeCog,
)
from ..acquisition.suppliers.insee.checks.parent_period_no_gaps import (
    CheckParentPeriodNoGapsAfterDownloadInseeCog,
)
from ..acquisition.suppliers.insee.checks.parent_period_overlap import (
    CheckParentPeriodOverlapAfterDownloadInseeCog,
)
from ..acquisition.suppliers.insee.checks.parent_uri_exist import (
    CheckParentURIsExistAfterDownloadInseeCog,
)
from ..acquisition.suppliers.insee.requests import (
    OutputPathsRequestCOG,
    RequestCOGArrondissementMunicipal,
    RequestCOGCommune,
    RequestCOGDepartement,
    RequestsCOGCollectivitesOutremer,
    RequestsCOGDistrict,
    RequestsCOGPays,
    RequestsCOGTerritoires,
)
from ..metadata import GeoCSVFileMetadata, InfoCurrentStoredData

logger = logging.getLogger(__name__)


def download_cog_data(
    acquisition_config: AcquisitionConfig,
    exceptions_handler_config: ErrorHandlerConfig,
    duckdb_conn: duckdb.DuckDBPyConnection,
    output_dir: Path,
    current_stored_data: InfoCurrentStoredData,
) -> None:
    """Download data from supplied URLs."""
    output_dir_insee = output_dir
    output_dir_insee.mkdir(parents=True, exist_ok=True)
    output_dir_insee_raw = output_dir_insee / "raw"
    output_dir_insee_raw.mkdir(parents=True, exist_ok=True)
    output_dir_insee_cleaned = output_dir_insee / "cleaned"
    output_dir_insee_cleaned.mkdir(parents=True, exist_ok=True)
    output_dir_insee_remove = output_dir_insee / "remove"
    output_dir_insee_remove.mkdir(parents=True, exist_ok=True)
    output_dir_insee_add_or_replace = output_dir_insee / "add_or_replace"
    output_dir_insee_add_or_replace.mkdir(parents=True, exist_ok=True)

    filenames_communes = "communes.csv"
    output_paths_communes = OutputPathsRequestCOG(
        raw_entities=output_dir_insee_raw / filenames_communes,
        add_or_replace_entities=output_dir_insee_add_or_replace / filenames_communes,
        remove_entities=output_dir_insee_remove / filenames_communes,
        cleaned_entities=output_dir_insee_cleaned / filenames_communes,
    )
    request_insee_commune = RequestCOGCommune(
        output_paths=output_paths_communes,
        acquisition_config=acquisition_config.insee,
        exceptions_handler_config=exceptions_handler_config.insee.communes,
    )
    filenames_arrondissements_municipaux = "arrondissements_municipaux.csv"
    output_paths_arrondissements_municipaux = OutputPathsRequestCOG(
        raw_entities=output_dir_insee_raw / filenames_arrondissements_municipaux,
        add_or_replace_entities=output_dir_insee_add_or_replace
        / filenames_arrondissements_municipaux,
        remove_entities=output_dir_insee_remove / filenames_arrondissements_municipaux,
        cleaned_entities=output_dir_insee_cleaned
        / filenames_arrondissements_municipaux,
    )
    request_insee_arrondissements_municipaux = RequestCOGArrondissementMunicipal(
        output_paths=output_paths_arrondissements_municipaux,
        acquisition_config=acquisition_config.insee,
        exceptions_handler_config=exceptions_handler_config.insee.arrondissements_municipaux,
    )
    filenames_departements = "departements.csv"
    output_paths_departements = OutputPathsRequestCOG(
        raw_entities=output_dir_insee_raw / filenames_departements,
        add_or_replace_entities=output_dir_insee_add_or_replace
        / filenames_departements,
        remove_entities=output_dir_insee_remove / filenames_departements,
        cleaned_entities=output_dir_insee_cleaned / filenames_departements,
    )
    request_insee_departements = RequestCOGDepartement(
        output_paths=output_paths_departements,
        acquisition_config=acquisition_config.insee,
        exceptions_handler_config=exceptions_handler_config.insee.departements,
    )
    filenames_collectivites_outremer = "collectivites_outremer.csv"
    output_paths_collectivites_outremer = OutputPathsRequestCOG(
        raw_entities=output_dir_insee_raw / filenames_collectivites_outremer,
        add_or_replace_entities=output_dir_insee_add_or_replace
        / filenames_collectivites_outremer,
        remove_entities=output_dir_insee_remove / filenames_collectivites_outremer,
        cleaned_entities=output_dir_insee_cleaned / filenames_collectivites_outremer,
    )
    request_insee_collectivites_outremer = RequestsCOGCollectivitesOutremer(
        output_paths=output_paths_collectivites_outremer,
        acquisition_config=acquisition_config.insee,
        exceptions_handler_config=exceptions_handler_config.insee.collectivites_outremer,
    )
    filenames_districts = "districts.csv"
    output_paths_districts = OutputPathsRequestCOG(
        raw_entities=output_dir_insee_raw / filenames_districts,
        add_or_replace_entities=output_dir_insee_add_or_replace / filenames_districts,
        remove_entities=output_dir_insee_remove / filenames_districts,
        cleaned_entities=output_dir_insee_cleaned / filenames_districts,
    )
    request_insee_districts = RequestsCOGDistrict(
        output_paths=output_paths_districts,
        acquisition_config=acquisition_config.insee,
        exceptions_handler_config=exceptions_handler_config.insee.districts,
    )
    filenames_pays = "pays.csv"
    output_paths_pays = OutputPathsRequestCOG(
        raw_entities=output_dir_insee_raw / filenames_pays,
        add_or_replace_entities=output_dir_insee_add_or_replace / filenames_pays,
        remove_entities=output_dir_insee_remove / filenames_pays,
        cleaned_entities=output_dir_insee_cleaned / filenames_pays,
    )
    request_insee_pays = RequestsCOGPays(
        output_paths=output_paths_pays,
        acquisition_config=acquisition_config.insee,
        exceptions_handler_config=exceptions_handler_config.insee.pays,
    )

    filenames_territoires = "territoires.csv"
    output_paths_territoires = OutputPathsRequestCOG(
        raw_entities=output_dir_insee_raw / filenames_territoires,
        add_or_replace_entities=output_dir_insee_add_or_replace / filenames_territoires,
        remove_entities=output_dir_insee_remove / filenames_territoires,
        cleaned_entities=output_dir_insee_cleaned / filenames_territoires,
    )
    request_insee_territoires = RequestsCOGTerritoires(
        output_paths=output_paths_territoires,
        acquisition_config=acquisition_config.insee,
        exceptions_handler_config=exceptions_handler_config.insee.territoires,
    )

    logger.info('Downloading "Communes" data from COG')
    try:
        request_insee_commune.send()
    except Exception as e:
        logger.error(f'Error downloading "Communes" data: {e}')
        raise RuntimeError(f'Failed to download "Communes" data: {e}') from e
    try:
        request_insee_commune.check_content(duckdb_conn=duckdb_conn)
    except Exception as e:
        logger.error(f'Error checking content of "Communes" data: {e}')
        raise RuntimeError(f'Failed to check content of "Communes" data: {e}') from e

    current_stored_data.add_data(
        key="insee_communes",
        value=GeoCSVFileMetadata(
            path=request_insee_commune.output_paths.cleaned_entities,
            header=True,
            delim=",",
            encoding="utf-8",
            colnames=request_insee_commune.colnames,
        ),
    )

    logger.info('Downloading "Arrondissements Municipaux" data from COG')
    try:
        request_insee_arrondissements_municipaux.send()
    except Exception as e:
        logger.error(f'Error downloading "Arrondissements Municipaux" data: {e}')
        raise RuntimeError(
            f'Failed to download "Arrondissements Municipaux" data: {e}'
        ) from e
    try:
        request_insee_arrondissements_municipaux.check_content(duckdb_conn=duckdb_conn)
    except Exception as e:
        logger.error(
            f'Error checking content of "Arrondissements Municipaux" data: {e}'
        )
        raise RuntimeError(
            f'Failed to check content of "Arrondissements Municipaux" data: {e}'
        ) from e
    current_stored_data.add_data(
        key="insee_arrondissements_municipaux",
        value=GeoCSVFileMetadata(
            path=request_insee_arrondissements_municipaux.output_paths.cleaned_entities,
            header=True,
            delim=",",
            encoding="utf-8",
            colnames=request_insee_arrondissements_municipaux.colnames,
        ),
    )

    logger.info('Downloading "Departements" data from COG')
    try:
        request_insee_departements.send()
    except Exception as e:
        logger.error(f'Error downloading "Departements" data: {e}')
        raise RuntimeError(f'Failed to download "Departements" data: {e}') from e
    try:
        request_insee_departements.check_content(duckdb_conn=duckdb_conn)
    except Exception as e:
        logger.error(f'Error checking content of "Departements" data: {e}')
        raise RuntimeError(
            f'Failed to check content of "Departements" data: {e}'
        ) from e
    current_stored_data.add_data(
        key="insee_departements",
        value=GeoCSVFileMetadata(
            path=request_insee_departements.output_paths.cleaned_entities,
            header=True,
            delim=",",
            encoding="utf-8",
            colnames=request_insee_departements.colnames,
        ),
    )

    logger.info('Downloading "Collectivités d\'Outre-mer" data from COG')
    try:
        request_insee_collectivites_outremer.send()
    except Exception as e:
        logger.error(f'Error downloading "Collectivités d\'Outre-mer" data: {e}')
        raise RuntimeError(
            f'Failed to download "Collectivités d\'Outre-mer" data: {e}'
        ) from e
    try:
        request_insee_collectivites_outremer.check_content(duckdb_conn=duckdb_conn)
    except Exception as e:
        logger.error(
            f'Error checking content of "Collectivités d\'Outre-mer" data: {e}'
        )
        raise RuntimeError(
            f'Failed to check content of "Collectivités d\'Outre-mer" data: {e}'
        ) from e
    current_stored_data.add_data(
        key="insee_collectivites_outremer",
        value=GeoCSVFileMetadata(
            path=request_insee_collectivites_outremer.output_paths.cleaned_entities,
            header=True,
            delim=",",
            encoding="utf-8",
            colnames=request_insee_collectivites_outremer.colnames,
        ),
    )

    logger.info('Downloading "Districts" data from COG')
    try:
        request_insee_districts.send()
    except Exception as e:
        logger.error(f'Error downloading "Districts" data: {e}')
        raise RuntimeError(f'Failed to download "Districts" data: {e}') from e
    try:
        request_insee_districts.check_content(duckdb_conn=duckdb_conn)
    except Exception as e:
        logger.error(f'Error checking content of "Districts" data: {e}')
        raise RuntimeError(f'Failed to check content of "Districts" data: {e}') from e
    current_stored_data.add_data(
        key="insee_districts",
        value=GeoCSVFileMetadata(
            path=request_insee_districts.output_paths.cleaned_entities,
            header=True,
            delim=",",
            encoding="utf-8",
            colnames=request_insee_districts.colnames,
        ),
    )

    logger.info('Downloading "Pays" data from COG')
    try:
        request_insee_pays.send()
    except Exception as e:
        logger.error(f'Error downloading "Pays" data: {e}')
        raise RuntimeError(f'Failed to download "Pays" data: {e}') from e
    try:
        request_insee_pays.check_content(duckdb_conn=duckdb_conn)
    except Exception as e:
        logger.error(f'Error checking content of "Pays" data: {e}')
        raise RuntimeError(f'Failed to check content of "Pays" data: {e}') from e
    current_stored_data.add_data(
        key="insee_pays",
        value=GeoCSVFileMetadata(
            path=request_insee_pays.output_paths.cleaned_entities,
            header=True,
            delim=",",
            encoding="utf-8",
            colnames=request_insee_pays.colnames,
        ),
    )

    logger.info('Downloading "Territoires" data from COG')
    try:
        request_insee_territoires.send()
    except Exception as e:
        logger.error(f'Error downloading "Territoires" data: {e}')
        raise RuntimeError(f'Failed to download "Territoires" data: {e}') from e
    try:
        request_insee_territoires.check_content(duckdb_conn=duckdb_conn)
    except Exception as e:
        logger.error(f'Error checking content of "Territoires" data: {e}')
        raise RuntimeError(f'Failed to check content of "Territoires" data: {e}') from e
    current_stored_data.add_data(
        key="insee_territoire",
        value=GeoCSVFileMetadata(
            path=request_insee_territoires.output_paths.cleaned_entities,
            header=True,
            delim=",",
            encoding="utf-8",
            colnames=request_insee_territoires.colnames,
        ),
    )

    requests_insee_list = [
        request_insee_commune,
        request_insee_arrondissements_municipaux,
        request_insee_departements,
        request_insee_collectivites_outremer,
        request_insee_districts,
        request_insee_pays,
        request_insee_territoires,
    ]

    logger.info(
        """
        Check, for the "Communes" data, the existence of URIs of the parent geographic entities
        (department or overseas collectivity).
        """
    )
    CheckParentURIsExistAfterDownloadInseeCog(
        parents_view_name=[
            request_insee_departements.view_name,
            request_insee_collectivites_outremer.view_name,
        ]
    ).run(request=request_insee_commune, duckdb_conn=duckdb_conn)
    logger.info(
        """
        Check, for the "Communes" data, that the validity periods
        of the parent geographic entities of a municipality do not overlap.
        """
    )
    CheckParentPeriodOverlapAfterDownloadInseeCog(
        parents_view_name=[
            request_insee_departements.view_name,
            request_insee_collectivites_outremer.view_name,
        ]
    ).run(request=request_insee_commune, duckdb_conn=duckdb_conn)
    logger.info(
        """
        Check, for the "Communes" data, that the union of the validity periods of
        the parent geographic entities of a municipality forms a continuous interval
        (i.e., there are no “gaps”).
        """
    )
    CheckParentPeriodNoGapsAfterDownloadInseeCog(
        parents_view_name=[
            request_insee_departements.view_name,
            request_insee_collectivites_outremer.view_name,
        ]
    ).run(request=request_insee_commune, duckdb_conn=duckdb_conn)
    logger.info(
        """
        Verify that, for the "Communes" data, the municipality’s validity period
        is indeed included in the union of the validity periods of
        its parent geographic entities.
        """
    )
    CheckParentPeriodsContainChildPeriodAfterDownloadInseeCog(
        parents_view_name=[
            request_insee_departements.view_name,
            request_insee_collectivites_outremer.view_name,
        ]
    ).run(request=request_insee_commune, duckdb_conn=duckdb_conn)
    logger.info(
        """
        Check, for "Arrondissements Municipaux" data, the existence of the
        URIs of the parent geographic entities (municipalities).'
        """
    )
    CheckParentURIsExistAfterDownloadInseeCog(
        parents_view_name=[request_insee_commune.view_name]
    ).run(request=request_insee_arrondissements_municipaux, duckdb_conn=duckdb_conn)
    logger.info(
        """
        Check, for the "Arrondissements Municipaux" data, that the validity periods
        of the parent geographic entities of a municipality do not overlap.'
        """
    )
    CheckParentPeriodOverlapAfterDownloadInseeCog(
        parents_view_name=[request_insee_commune.view_name]
    ).run(request=request_insee_arrondissements_municipaux, duckdb_conn=duckdb_conn)
    logger.info(
        """
        Check, for the "Arrondissements Municipaux" data, that the union of the validity periods
        of the parent geographic entities of a municipality
        forms a continuous interval (i.e., there are no “gaps”).'
        """
    )
    CheckParentPeriodNoGapsAfterDownloadInseeCog(
        parents_view_name=[request_insee_commune.view_name]
    ).run(request=request_insee_arrondissements_municipaux, duckdb_conn=duckdb_conn)
    logger.info(
        """
        Verify that, for the "Arrondissements Municipaux" data, the municipality’s
        validity period is indeed included in the union
        of the validity periods of its parent geographic entities.'
        """
    )
    CheckParentPeriodsContainChildPeriodAfterDownloadInseeCog(
        parents_view_name=[request_insee_commune.view_name]
    ).run(request=request_insee_arrondissements_municipaux, duckdb_conn=duckdb_conn)
    logger.info(
        """
        Check that the URIs of all geographic events are
        associated with only a single, unique event date.
        """
    )
    CheckEventsConsistencyAfterDownloadInseeCog().run(
        requests=requests_insee_list, duckdb_conn=duckdb_conn
    )
    logger.info(
        """
        Verify that there are no overlapping periods for a given INSEE code
        (regardless of the type of geographical entity), i.e., that there are not two URIs
        associated with the same INSEE code whose validity periods intersect.
        """
    )
    CheckGlobalInseeCodeOverlapAfterDownloadInseeCog().run(
        requests=requests_insee_list, duckdb_conn=duckdb_conn
    )
