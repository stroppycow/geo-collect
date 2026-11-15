import csv
import logging
from abc import ABC
from pathlib import Path

import pystache
import requests
from duckdb import DuckDBPyConnection
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ....metadata import (
    ColumnDataType,
    DateColumnDataType,
    GeometryColumnDataType,
    IntegerColumnDataType,
    StringColumnDataType,
)
from .checks.abstract import DataValidationAndConsistencyAdminExpressCog
from .checks.geometry_valid import CheckGeometryValidAfterDownloadAdminExpressCog
from .checks.not_null import CheckNotNullAfterDownloadAdminExpressCog
from .checks.parsing import CheckParsingAfterDownloadAdminExpressCog
from .checks.pattern import CheckPatternAfterDownloadAdminExpressCog
from .checks.unique_value import CheckUniqueValueAfterDownloadAdminExpressCog
from .config import (
    AdminExpressGeoAddOrReplace,
    AdminExpressGeoRemove,
    AdminExpressLayerExceptionsToIgnoreOrCorrect,
    AdminExpressSupplierConfig,
)

logger = logging.getLogger(__name__)

# INSEE code of a commune or municipal arrondissement: 2 digits (or 2A/2B for
# Corsica) + 3 characters, including DROM (971-978).
_INSEE_CODE_PATTERN = r"^((0[1-9]|[1-8][0-9]|9[0-8]|2[AB])[0-9]{3})$"

# code_insee of the departement layer: department number (2A/2B for Corsica)
# or 3-digit DROM code (971-976).
_DEPARTEMENT_CODE_PATTERN = r"^(0[1-9]|[1-8][0-9]|9[0-6]|2[AB]|97[1-6])$"

# code_insee of the collectivite_territoriale layer uses the IGN coding of
# territorial collectivities: e.g. "75C" (Paris), "6AE" (Alsace),
# "20R" (Corsica), "975" (Saint-Pierre-et-Miquelon), "971D" (Guadeloupe).
_COLLECTIVITE_TERRITORIALE_CODE_PATTERN = (
    r"^([0-9]{2}[A-Z]|[0-9][A-Z]{2}|[0-9]{3}[A-Z]?)$"
)

# Unique key of an Admin Express entity (e.g. "COMMUNE_0000000000027067").
_CLEABS_PATTERN = r"^[A-Z0-9_]+$"

# Cache of the catalog resolutions, so that the catalog is queried only once
# per run even if several layers are downloaded.
_edition_cache: dict[tuple[str, str, str], str] = {}
_files_cache: dict[str, dict[str, str]] = {}


def _new_session(acquisition_config: AdminExpressSupplierConfig) -> requests.Session:
    retry_strategy = Retry(
        total=acquisition_config.max_retries,
        backoff_factor=acquisition_config.backoff_factor,
        status_forcelist=[408, 429, 500, 502, 503, 504],
        redirect=0,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _get_json(
    session: requests.Session,
    url: str,
    acquisition_config: AdminExpressSupplierConfig,
    description: str,
) -> dict:
    try:
        with session.get(
            url=url,
            headers={"Accept": "application/json"},
            timeout=(
                acquisition_config.read_timeout,
                acquisition_config.connect_timeout,
            ),
        ) as response:
            if response.status_code != 200:
                raise requests.exceptions.HTTPError(
                    f"HTTP error while querying {description}: "
                    f"{response.status_code} - {response.text}"
                )
            return response.json()
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Timeout occurred while querying {description}") from e
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Connection error while querying {description}") from e
    except requests.exceptions.HTTPError:
        raise
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Request error while querying {description}") from e


def _entries_of(feed: dict) -> list[dict]:
    entries = feed.get("entry", [])
    if isinstance(entries, dict):
        return [entries]
    return entries


def resolve_latest_edition(acquisition_config: AdminExpressSupplierConfig) -> str:
    """Resolve the latest Admin Express COG edition available in the catalog
    for the configured zone and format.

    Returns the edition name, e.g.
    ``ADMIN-EXPRESS-COG_4-0__GEOPARQUET_WGS84G_FRA_2026-01-01``.
    """
    cache_key = (
        acquisition_config.catalog_url,
        acquisition_config.zone,
        acquisition_config.format,
    )
    if cache_key in _edition_cache:
        return _edition_cache[cache_key]
    session = _new_session(acquisition_config)
    url = (
        f"{acquisition_config.catalog_url}?zone={acquisition_config.zone}"
        f"&format={acquisition_config.format}&page=1&limit=50"
    )
    feed = _get_json(
        session=session,
        url=url,
        acquisition_config=acquisition_config,
        description=f"Admin Express COG catalog ({acquisition_config.zone}/"
        f"{acquisition_config.format})",
    )
    entries = _entries_of(feed)
    if not entries:
        raise RuntimeError(
            f"No {acquisition_config.format} edition found for zone "
            f"{acquisition_config.zone} in catalog {acquisition_config.catalog_url}"
        )
    latest = max(entries, key=lambda e: e.get("editionDate") or "")
    edition = latest["title"]
    logger.info(
        f"Latest Admin Express COG edition for zone {acquisition_config.zone} "
        f"and format {acquisition_config.format}: {edition} "
        f"(edition date {latest.get('editionDate')})"
    )
    _edition_cache[cache_key] = edition
    return edition


def get_layer_download_url(
    acquisition_config: AdminExpressSupplierConfig, layer_filename: str
) -> str:
    """Return the direct download URL of a layer file (e.g. ``commune.parquet``)
    in the latest edition resolved from the catalog."""
    edition = resolve_latest_edition(acquisition_config)
    if edition not in _files_cache:
        session = _new_session(acquisition_config)
        files: dict[str, str] = {}
        page = 1
        while True:
            url = f"{acquisition_config.catalog_url}/{edition}?page={page}&limit=50"
            feed = _get_json(
                session=session,
                url=url,
                acquisition_config=acquisition_config,
                description=f"Admin Express COG edition {edition}",
            )
            for entry in _entries_of(feed):
                for link in entry.get("link", []):
                    href = link.get("href")
                    if isinstance(href, str) and href.endswith(".parquet"):
                        files[href.rsplit("/", 1)[1]] = href
            pagecount = feed.get("pagecount", 1)
            if page >= pagecount:
                break
            page += 1
        _files_cache[edition] = files
    files = _files_cache[edition]
    if layer_filename not in files:
        raise RuntimeError(
            f"File '{layer_filename}' not found in Admin Express COG edition "
            f"{edition} (available: {sorted(files)})"
        )
    return files[layer_filename]


class TemplatesSQLRequestAdminExpressCog:
    def __init__(
        self,
        copy: str | Path,
        create_view: str | Path,
        update: str | Path,
    ):
        if isinstance(copy, str):
            self.copy = Path(copy)
        else:
            self.copy = copy
        if isinstance(create_view, str):
            self.create_view = Path(create_view)
        else:
            self.create_view = create_view
        if isinstance(update, str):
            self.update = Path(update)
        else:
            self.update = update


class OutputPathsRequestAdminExpressCog:
    def __init__(
        self,
        raw_entities: str | Path,
        add_or_replace_entities: str | Path,
        remove_entities: str | Path,
        cleaned_entities: str | Path,
    ):
        if isinstance(raw_entities, str):
            self.raw_entities = Path(raw_entities)
        else:
            self.raw_entities = raw_entities
        if isinstance(add_or_replace_entities, str):
            self.add_or_replace_entities = Path(add_or_replace_entities)
        else:
            self.add_or_replace_entities = add_or_replace_entities
        if isinstance(remove_entities, str):
            self.remove_entities = Path(remove_entities)
        else:
            self.remove_entities = remove_entities
        if isinstance(cleaned_entities, str):
            self.cleaned_entities = Path(cleaned_entities)
        else:
            self.cleaned_entities = cleaned_entities


class RequestAdminExpressCog(ABC):
    """Abstract base class to download one layer of the IGN Admin Express COG
    product (GeoParquet, France entière) from the Géoplateforme catalog."""

    def __init__(
        self,
        output_paths: OutputPathsRequestAdminExpressCog,
        layer_filename: str,
        description: str,
        view_name: str,
        exceptions_handler_config: AdminExpressLayerExceptionsToIgnoreOrCorrect,
        sql_templates: TemplatesSQLRequestAdminExpressCog,
        acquisition_config: AdminExpressSupplierConfig,
        colnames: list[ColumnDataType],
        extra_controls: list[DataValidationAndConsistencyAdminExpressCog],
    ):
        self.output_paths = output_paths
        self.layer_filename = layer_filename
        self.description = description
        self.view_name = view_name
        self.exceptions_handler_config = exceptions_handler_config
        self.sql_templates = sql_templates
        self.acquisition_config = acquisition_config
        self.colnames = colnames
        self.extra_controls = extra_controls

    def send(self) -> None:
        """Download the raw GeoParquet file of the layer."""
        url = get_layer_download_url(
            acquisition_config=self.acquisition_config,
            layer_filename=self.layer_filename,
        )
        session = _new_session(self.acquisition_config)
        logger.info(f"Downloading {self.description} from {url}")
        try:
            with session.get(
                url=url,
                data=None,
                stream=True,
                timeout=(
                    self.acquisition_config.read_timeout,
                    self.acquisition_config.connect_timeout,
                ),
            ) as response:
                if response.status_code != 200:
                    raise requests.exceptions.HTTPError(
                        f"HTTP error while downloading {self.description}: "
                        f"{response.status_code}"
                    )

                if not self.output_paths.raw_entities.parent.exists():
                    self.output_paths.raw_entities.parent.mkdir(
                        parents=True, exist_ok=True
                    )
                if self.output_paths.raw_entities.exists():
                    self.output_paths.raw_entities.unlink()

                with open(self.output_paths.raw_entities, "wb") as foutput:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            foutput.write(chunk)

        except requests.exceptions.Timeout as e:
            raise TimeoutError(
                f"Timeout occurred while downloading {self.description}"
            ) from e
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(
                f"Connection error while downloading {self.description}"
            ) from e
        except requests.exceptions.HTTPError:
            raise
        except requests.exceptions.RequestException as e:
            raise RuntimeError(
                f"Request error while downloading {self.description}"
            ) from e
        except Exception as e:
            raise RuntimeError(
                f"Unexpected error while downloading {self.description}"
            ) from e

    def apply_updates(self, duckdb_conn: DuckDBPyConnection):
        """Apply the declared exceptions (add_or_replace / remove, keyed by
        cleabs) before the content checks."""
        add_or_replace_list = [
            exception
            for exception in self.exceptions_handler_config.root
            if isinstance(exception, AdminExpressGeoAddOrReplace)
        ]

        if not self.output_paths.add_or_replace_entities.parent.exists():
            self.output_paths.add_or_replace_entities.parent.mkdir(
                parents=True, exist_ok=True
            )
        if self.output_paths.add_or_replace_entities.exists():
            self.output_paths.add_or_replace_entities.unlink()

        with open(
            self.output_paths.add_or_replace_entities,
            mode="w",
            newline="",
            encoding="utf-8",
        ) as f_add_or_replace:
            fieldnames_add_or_replace = [x.name for x in self.colnames]
            writer_add_or_replace = csv.DictWriter(
                f_add_or_replace, fieldnames=fieldnames_add_or_replace
            )
            writer_add_or_replace.writeheader()
            for exception in add_or_replace_list:
                row = {name: "" for name in fieldnames_add_or_replace}
                row.update(exception.to_dict_csv_export())
                writer_add_or_replace.writerow(row)

        remove_list = [
            exception
            for exception in self.exceptions_handler_config.root
            if isinstance(exception, AdminExpressGeoRemove)
        ]

        if not self.output_paths.remove_entities.parent.exists():
            self.output_paths.remove_entities.parent.mkdir(parents=True, exist_ok=True)
        if self.output_paths.remove_entities.exists():
            self.output_paths.remove_entities.unlink()

        with open(
            self.output_paths.remove_entities, mode="w", newline="", encoding="utf-8"
        ) as f_remove:
            fieldnames_remove = ["cleabs"]
            writer_remove = csv.DictWriter(f_remove, fieldnames=fieldnames_remove)
            writer_remove.writeheader()
            for exception in remove_list:
                writer_remove.writerow({"cleabs": exception.cleabs})

        output_path_tmp = self.output_paths.cleaned_entities.with_suffix(".tmp")
        if not self.output_paths.cleaned_entities.parent.exists():
            self.output_paths.cleaned_entities.parent.mkdir(parents=True, exist_ok=True)
        if output_path_tmp.exists():
            output_path_tmp.unlink()

        context_apply_updates: dict[str, str] = {
            "view_name": self.view_name,
            "path_add_or_replace": str(
                self.output_paths.add_or_replace_entities.resolve()
            ),
            "path_remove": str(self.output_paths.remove_entities.resolve()),
            "output_path": str(output_path_tmp.resolve()),
        }

        renderer_apply_updates = pystache.Renderer(escape=lambda s: s)
        try:
            with open(
                self.sql_templates.update, "r", encoding="utf-8"
            ) as template_apply_updates_path:
                template_apply_updates_content = template_apply_updates_path.read()
        except Exception as e:
            raise RuntimeError(
                f"Failed to load template file {self.sql_templates.update}"
            ) from e

        try:
            renderer_apply_updates_str = renderer_apply_updates.render(
                template_apply_updates_content, context_apply_updates
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to render template file {self.sql_templates.update}"
            ) from e

        try:
            duckdb_conn.execute(renderer_apply_updates_str)
            if self.output_paths.cleaned_entities.exists():
                self.output_paths.cleaned_entities.unlink()
            output_path_tmp.replace(self.output_paths.cleaned_entities)
        except Exception as e:
            raise RuntimeError(
                f"Failed to execute SQL script {self.sql_templates.update}"
            ) from e

    def check_content(self, duckdb_conn: DuckDBPyConnection) -> None:
        """Check if the content of the file is valid"""
        logger.info(f"Checking content of {self.description} data after downloading")
        controls: list[DataValidationAndConsistencyAdminExpressCog] = [
            CheckParsingAfterDownloadAdminExpressCog()
        ]
        controls.extend(self.extra_controls)
        nb_controls = len(controls)
        if nb_controls == 0:
            logger.info(
                f"No control to run for {self.description} data after downloading"
            )
        for current_step, control in enumerate(controls):
            logger.info(
                f"Running check {current_step + 1}/{nb_controls}: "
                f"{type(control).__name__}"
            )
            control.run(request=self, duckdb_conn=duckdb_conn)
        logger.info(f"All checks passed for {self.description} data after downloading")


class _AdminExpressLayerRequest(RequestAdminExpressCog):
    """Base class wiring the per-layer SQL templates and the standard checks."""

    layer_filename: str

    def __init__(
        self,
        output_paths: OutputPathsRequestAdminExpressCog,
        description: str,
        view_name: str,
        exceptions_handler_config: AdminExpressLayerExceptionsToIgnoreOrCorrect,
        acquisition_config: AdminExpressSupplierConfig,
        colnames: list[ColumnDataType],
        not_null_colnames: list[str],
        code_insee_pattern: str = _INSEE_CODE_PATTERN,
    ):
        sql_dir = Path(__file__).parent / "sql"
        layer = self.layer_filename.removesuffix(".parquet")
        super().__init__(
            output_paths=output_paths,
            layer_filename=self.layer_filename,
            description=description,
            view_name=view_name,
            exceptions_handler_config=exceptions_handler_config,
            sql_templates=TemplatesSQLRequestAdminExpressCog(
                copy=sql_dir / f"{layer}_copy.mustache.sql",
                create_view=sql_dir / f"{layer}_import.mustache.sql",
                update=sql_dir / f"{layer}_correct.mustache.sql",
            ),
            acquisition_config=acquisition_config,
            colnames=colnames,
            extra_controls=[
                CheckPatternAfterDownloadAdminExpressCog(
                    colname="cleabs", pattern=_CLEABS_PATTERN
                ),
                CheckPatternAfterDownloadAdminExpressCog(
                    colname="code_insee", pattern=code_insee_pattern
                ),
                CheckUniqueValueAfterDownloadAdminExpressCog(colname="cleabs"),
                CheckUniqueValueAfterDownloadAdminExpressCog(colname="code_insee"),
            ]
            + [
                CheckNotNullAfterDownloadAdminExpressCog(colname=colname)
                for colname in not_null_colnames
            ]
            + [CheckGeometryValidAfterDownloadAdminExpressCog()],
        )


class RequestAdminExpressCollectiviteTerritoriale(_AdminExpressLayerRequest):
    """Class to download the "collectivite_territoriale" layer of Admin Express COG"""

    layer_filename = "collectivite_territoriale.parquet"

    def __init__(
        self,
        output_paths: OutputPathsRequestAdminExpressCog,
        acquisition_config: AdminExpressSupplierConfig,
        exceptions_handler_config: AdminExpressLayerExceptionsToIgnoreOrCorrect,
    ):
        super().__init__(
            output_paths=output_paths,
            description='"Collectivités territoriales" data',
            view_name="adminexpress_collectivites_territoriales",
            exceptions_handler_config=exceptions_handler_config,
            acquisition_config=acquisition_config,
            colnames=[
                IntegerColumnDataType(name="fid"),
                StringColumnDataType(name="cleabs"),
                StringColumnDataType(name="nom_officiel"),
                StringColumnDataType(name="nom_officiel_en_majuscules"),
                StringColumnDataType(name="code_insee"),
                StringColumnDataType(name="code_insee_de_la_region"),
                StringColumnDataType(name="code_siren"),
                GeometryColumnDataType(name="geometrie"),
            ],
            not_null_colnames=["cleabs", "code_insee", "nom_officiel", "geometrie"],
            code_insee_pattern=_COLLECTIVITE_TERRITORIALE_CODE_PATTERN,
        )


class RequestAdminExpressDepartement(_AdminExpressLayerRequest):
    """Class to download the "departement" layer of Admin Express COG"""

    layer_filename = "departement.parquet"

    def __init__(
        self,
        output_paths: OutputPathsRequestAdminExpressCog,
        acquisition_config: AdminExpressSupplierConfig,
        exceptions_handler_config: AdminExpressLayerExceptionsToIgnoreOrCorrect,
    ):
        super().__init__(
            output_paths=output_paths,
            description='"Départements" data',
            view_name="adminexpress_departements",
            exceptions_handler_config=exceptions_handler_config,
            acquisition_config=acquisition_config,
            colnames=[
                IntegerColumnDataType(name="fid"),
                StringColumnDataType(name="cleabs"),
                StringColumnDataType(name="nom_officiel"),
                StringColumnDataType(name="nom_officiel_en_majuscules"),
                StringColumnDataType(name="code_insee"),
                StringColumnDataType(name="code_insee_de_la_region"),
                StringColumnDataType(name="code_siren"),
                GeometryColumnDataType(name="geometrie"),
            ],
            not_null_colnames=["cleabs", "code_insee", "nom_officiel", "geometrie"],
            code_insee_pattern=_DEPARTEMENT_CODE_PATTERN,
        )


class RequestAdminExpressCommune(_AdminExpressLayerRequest):
    """Class to download the "commune" layer of Admin Express COG"""

    layer_filename = "commune.parquet"

    def __init__(
        self,
        output_paths: OutputPathsRequestAdminExpressCog,
        acquisition_config: AdminExpressSupplierConfig,
        exceptions_handler_config: AdminExpressLayerExceptionsToIgnoreOrCorrect,
    ):
        super().__init__(
            output_paths=output_paths,
            description='"Communes" data',
            view_name="adminexpress_communes",
            exceptions_handler_config=exceptions_handler_config,
            acquisition_config=acquisition_config,
            colnames=[
                IntegerColumnDataType(name="fid"),
                StringColumnDataType(name="cleabs"),
                StringColumnDataType(name="nom_officiel"),
                StringColumnDataType(name="nom_officiel_en_majuscules"),
                StringColumnDataType(name="statut"),
                StringColumnDataType(name="code_insee"),
                IntegerColumnDataType(name="population"),
                DateColumnDataType(name="date_du_recensement", format="%Y-%m-%d"),
                StringColumnDataType(name="organisme_recenseur"),
                StringColumnDataType(name="code_insee_du_canton"),
                StringColumnDataType(name="code_insee_de_l_arrondissement"),
                StringColumnDataType(name="code_insee_du_departement"),
                StringColumnDataType(name="code_insee_de_la_region"),
                StringColumnDataType(name="code_siren"),
                StringColumnDataType(name="codes_siren_des_epci"),
                StringColumnDataType(name="code_postal"),
                IntegerColumnDataType(name="superficie_cadastrale"),
                GeometryColumnDataType(name="geometrie"),
            ],
            not_null_colnames=["cleabs", "code_insee", "nom_officiel", "geometrie"],
        )


class RequestAdminExpressArrondissementMunicipal(_AdminExpressLayerRequest):
    """Class to download the "arrondissement_municipal" layer of Admin Express COG"""

    layer_filename = "arrondissement_municipal.parquet"

    def __init__(
        self,
        output_paths: OutputPathsRequestAdminExpressCog,
        acquisition_config: AdminExpressSupplierConfig,
        exceptions_handler_config: AdminExpressLayerExceptionsToIgnoreOrCorrect,
    ):
        super().__init__(
            output_paths=output_paths,
            description='"Arrondissements municipaux" data',
            view_name="adminexpress_arrondissements_municipaux",
            exceptions_handler_config=exceptions_handler_config,
            acquisition_config=acquisition_config,
            colnames=[
                IntegerColumnDataType(name="fid"),
                StringColumnDataType(name="cleabs"),
                StringColumnDataType(name="nom_officiel"),
                StringColumnDataType(name="nom_officiel_en_majuscules"),
                StringColumnDataType(name="numero_de_l_arrondissement_municipal"),
                StringColumnDataType(name="code_insee"),
                StringColumnDataType(name="code_insee_de_la_commune_de_rattach"),
                StringColumnDataType(name="code_postal"),
                IntegerColumnDataType(name="population"),
                GeometryColumnDataType(name="geometrie"),
            ],
            not_null_colnames=["cleabs", "code_insee", "nom_officiel", "geometrie"],
        )


class RequestAdminExpressCommuneAssocieeOuDeleguee(_AdminExpressLayerRequest):
    """Class to download the "commune_associee_ou_deleguee" layer of Admin Express COG"""

    layer_filename = "commune_associee_ou_deleguee.parquet"

    def __init__(
        self,
        output_paths: OutputPathsRequestAdminExpressCog,
        acquisition_config: AdminExpressSupplierConfig,
        exceptions_handler_config: AdminExpressLayerExceptionsToIgnoreOrCorrect,
    ):
        super().__init__(
            output_paths=output_paths,
            description='"Communes associées ou déléguées" data',
            view_name="adminexpress_communes_associees_ou_deleguees",
            exceptions_handler_config=exceptions_handler_config,
            acquisition_config=acquisition_config,
            colnames=[
                IntegerColumnDataType(name="fid"),
                StringColumnDataType(name="cleabs"),
                StringColumnDataType(name="nom_officiel"),
                StringColumnDataType(name="nom_officiel_en_majuscules"),
                StringColumnDataType(name="code_insee"),
                StringColumnDataType(name="code_insee_de_la_commune_de_rattach"),
                StringColumnDataType(name="code_postal"),
                StringColumnDataType(name="code_siren"),
                StringColumnDataType(name="nature"),
                IntegerColumnDataType(name="population"),
                GeometryColumnDataType(name="geometrie"),
            ],
            not_null_colnames=["cleabs", "code_insee", "nom_officiel", "geometrie"],
        )
