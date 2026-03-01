from pathlib import Path
from typing import Union, Optional
from abc import ABC
from urllib.parse import quote_plus
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from duckdb import DuckDBPyConnection
import logging
import pystache
import csv


from .config import InseeSupplierConfig, InseeExceptionsToIgnoreOrCorrectModel, CommunesInseeExceptionsToIgnoreOrCorrect, ArrondissementsMunicipauxInseeExceptionsToIgnoreOrCorrect, DepartementsInseeExceptionsToIgnoreOrCorrect, CollectivitesDOutreMerInseeExceptionsToIgnoreOrCorrect, DistrictsInseeExceptionsToIgnoreOrCorrect, PaysInseeExceptionsToIgnoreOrCorrect
from .checks.abstract import DataValidationAndConsistencyInseeCog
from .checks.date_consistency import CheckDateConsistencyAfterDownloadInseeCog
from .checks.insee_code_overlap import CheckInseeCodeOverlapAfterDownloadInseeCog
from .checks.parsing import CheckParsingAfterDownloadInseeCog
from .checks.start_date import CheckStartDateAfterDownloadInseeCog
from .checks.end_date import CheckEndDateAfterDownloadInseeCog
from .checks.pattern import CheckPatternAfterDownloadInseeCog
from .checks.uri_unicity import CheckURIUnicityAfterDownloadInseeCog
from .checks.end_event_consistency import CheckEndEventConsistencyAfterDownloadInseeCog
from .checks.events_unequal import CheckEventsUnequalAfterDownloadInseeCog
from .checks.apply_update import InseeGeoRemove, InseeGeoAddOrReplace


class TemplatesSQLRequestCOG:
    def __init__(
            self,
            copy: Union[str, Path],
            create_view: Union[str, Path],
            update: Union[str, Path],
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

class OutputPathsRequestCOG:
    def __init__(
            self,
            raw_entities: Union[str, Path],
            add_or_replace_entities: Union[str, Path],
            remove_entities: Union[str, Path],
            cleaned_entities: Union[str, Path]
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


class RequestCOG(ABC):
    """Abstract base class for querying the Official geographic code alias COG (Code officiel géographique)"""
    headers = {"Content-type": "application/x-www-form-urlencoded"}

    def __init__(
            self,
            output_paths: OutputPathsRequestCOG,
            request: Union[str, Path],
            description: str,
            view_name: str,
            exceptions_handler_config: InseeExceptionsToIgnoreOrCorrectModel,
            sql_templates: TemplatesSQLRequestCOG,
            acquisition_config: InseeSupplierConfig = InseeSupplierConfig(),
            colnames: list[str] = [],
            extra_controls: list[DataValidationAndConsistencyInseeCog] = []
            
        ):
        self.output_paths = output_paths
        self.request = request
        self.description = description
        self.view_name = view_name
        self.exceptions_handler_config = exceptions_handler_config
        self.sql_templates = sql_templates
        self.acquisition_config = acquisition_config
        self.colnames = colnames
        self.extra_controls = extra_controls

    def send(self) -> None:
        request_str : Optional[str] = None

        if isinstance(self.request, Path) or isinstance(self.request, str):
            try:
                
                with open(self.request, 'r', encoding='utf-8') as file:
                    request_str = file.read()
            except Exception as e:
                raise RuntimeError(f"Failed to read file {self.request}") from e
        else:
            if Path(self.request).exists():
                try:
                    with open(self.request, 'r', encoding='utf-8') as file:
                        request_str = file.read()
                except Exception as e:
                    raise RuntimeError(f"Failed to read file {self.request}") from e
            else:
                request_str = self.request

        try:
            retry_strategy = Retry(
                total=self.acquisition_config.max_retries,
                backoff_factor=self.acquisition_config.backoff_factor,
                status_forcelist=[408, 429, 500, 502, 503, 504],
                redirect=0
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session = requests.Session()
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            with session.post(
                url=self.acquisition_config.endpoint_url,
                data="format=text/csv&query="+quote_plus(request_str),
                headers=RequestCOG.headers,
                timeout=(self.acquisition_config.read_timeout, self.acquisition_config.connect_timeout)
            ) as response:
                if response.status_code != 200:
                    raise requests.exceptions.HTTPError(f"HTTP error while querying {self.description} from COG: {response.status_code} - {response.text}")
                
                if not self.output_paths.raw_entities.parent.exists():
                    self.output_paths.raw_entities.parent.mkdir(parents=True, exist_ok=True)
                if self.output_paths.raw_entities.exists():
                   self.output_paths.raw_entities.unlink()

                with open(self.output_paths.raw_entities, "wb") as foutput:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            foutput.write(chunk)


        except requests.exceptions.Timeout as e:
            raise TimeoutError(f"Timeout occurred while querying {self.description}") from e
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Connection error while querying {self.description}") from e
        except requests.exceptions.HTTPError as e:
            raise e
        except requests.exceptions.RequestException  as e:
            raise RuntimeError(f"Request error while querying {self.description}") from e
        except Exception as e:
            raise RuntimeError(f"Unexpected error while querying {self.description}") from e
        
    def apply_updates(self, duckdb_conn: DuckDBPyConnection):
        """Apply updates before checks"""
        uri_add_or_update_list = [exception for exception in self.exceptions_handler_config.root if isinstance(exception, InseeGeoAddOrReplace)]
        
        if not self.output_paths.add_or_replace_entities.parent.exists():
            self.output_paths.add_or_replace_entities.parent.mkdir(parents=True, exist_ok=True)
        if self.output_paths.add_or_replace_entities.exists():
            self.output_paths.add_or_replace_entities.unlink()
        
        with open(self.output_paths.add_or_replace_entities, mode="w", newline="", encoding="utf-8") as f_add_or_replace:
            fieldnames_add_or_replace = self.colnames
            writer_add_or_replace = csv.DictWriter(f_add_or_replace, fieldnames=fieldnames_add_or_replace)
            writer_add_or_replace.writeheader()
            for exception in uri_add_or_update_list:
                writer_add_or_replace.writerow(exception.to_dict_csv_export())
       
        uri_removal_list = [exception for exception in self.exceptions_handler_config.root if isinstance(exception, InseeGeoRemove)]

        if not self.output_paths.remove_entities.parent.exists():
            self.output_paths.remove_entities.parent.mkdir(parents=True, exist_ok=True)
        if self.output_paths.remove_entities.exists():
            self.output_paths.remove_entities.unlink()

        with open(self.output_paths.remove_entities, mode="w", newline="", encoding="utf-8") as f_remove:
            fieldnames_remove = ['uri']
            writer_remove = csv.DictWriter(f_remove, fieldnames=fieldnames_remove)
            writer_remove.writeheader()
            for exception in uri_removal_list:
                writer_remove.writerow({'uri': exception.uri})
        
        output_path_tmp =  self.output_paths.cleaned_entities.with_suffix(".tmp")
        if not self.output_paths.cleaned_entities.parent.exists():
            self.output_paths.cleaned_entities.parent.mkdir(parents=True, exist_ok=True)
        if output_path_tmp.exists():
            output_path_tmp.unlink()

        context_apply_updates: dict[str, str] = {
            "view_name": self.view_name,
            "path_add_or_replace": str(self.output_paths.add_or_replace_entities.resolve()),
            "path_remove": str(self.output_paths.remove_entities.resolve()),
            "output_path": str(output_path_tmp.resolve())
        }

        renderer_apply_updates = pystache.Renderer(escape=lambda s: s)
        try:
            with open(self.sql_templates.update, 'r', encoding='utf-8') as template_apply_updates_path:
                template_apply_updates_content = template_apply_updates_path.read()
        except Exception as e:
            raise RuntimeError(f"Failed to load template file {self.sql_templates.update}") from e
        
        try:
            renderer_apply_updates_str = renderer_apply_updates.render(template_apply_updates_content, context_apply_updates)
        except Exception as e:
            raise RuntimeError(f"Failed to render template file {self.sql_templates.update}") from e
        
        try:
            duckdb_conn.execute(renderer_apply_updates_str)
            if self.output_paths.cleaned_entities.exists():
                self.output_paths.cleaned_entities.unlink()
            output_path_tmp.replace(self.output_paths.cleaned_entities)
        except Exception as e:
            raise RuntimeError(f"Failed to execute SQL script {self.sql_templates.update}") from e 
               
        
    def check_content(self, duckdb_conn : DuckDBPyConnection) -> None:
        """Check if the content of the file is valid"""
        logging.info(f"Checking content of {self.description} after downloading")
        controls: list[DataValidationAndConsistencyInseeCog] = [CheckParsingAfterDownloadInseeCog()]
        controls.extend(self.extra_controls)
        nb_controls = len(controls)
        if nb_controls == 0:
            logging.info(f"No control to run for {self.description} after downloading")
        for current_step, control in enumerate(controls):
            logging.info(f"Running check {current_step+1}/{nb_controls}: {type(control).__name__}")
            control.run(request=self, duckdb_conn=duckdb_conn)
        logging.info(f"All checks passed for {self.description} after downloading")

class RequestCOGCommune(RequestCOG):
    """Class to query all communes from the COG"""
    def __init__(
            self,
            output_paths: OutputPathsRequestCOG,
            acquisition_config: InseeSupplierConfig = InseeSupplierConfig(),
            exceptions_handler_config: CommunesInseeExceptionsToIgnoreOrCorrect = CommunesInseeExceptionsToIgnoreOrCorrect()
        ):
        super().__init__(
            output_paths=output_paths,
            request=Path(__file__).parent / "requests" / "communes.rq",
            description='"Communes" data',
            view_name="insee_communes",
            exceptions_handler_config=exceptions_handler_config,
            acquisition_config=acquisition_config,
            sql_templates= TemplatesSQLRequestCOG(
                copy=Path(__file__).parent / "sql" / "communes_copy.mustache.sql",
                create_view=Path(__file__).parent / "sql" / "communes_import.mustache.sql",
                update=Path(__file__).parent / "sql" / "communes_correct.mustache.sql"
            ),
            colnames=[
                'uri',
                'insee_code',
                'label',
                'article_code',
                'parent_uri',
                'start_event_uri',
                'end_event_uri',
                'start_date',
                'end_date',
                'parent_uri_count',
                'start_date_count',
                'end_date_count'
            ],
            extra_controls = [
                CheckPatternAfterDownloadInseeCog(colname="uri", pattern=r"^http://id.insee.fr/geo/commune/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$"),
                CheckURIUnicityAfterDownloadInseeCog(),
                CheckPatternAfterDownloadInseeCog(colname="insee_code", pattern=r"^(0[1-9]|[1-8][0-9]|9[0-8]|2[AB])[0-9]{3}$"),
                CheckPatternAfterDownloadInseeCog(colname="article_code", pattern=r"^[0-8X]$"),
                CheckPatternAfterDownloadInseeCog(colname="parent_uri", pattern=r"^(http://id.insee.fr/geo/(departement|collectiviteDOutreMer)/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12})([|]http://id.insee.fr/geo/(departement|collectiviteDOutreMer)/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12})*$"),
                CheckPatternAfterDownloadInseeCog(colname="start_event_uri", pattern=r"^http://id.insee.fr/geo/evenementGeographique/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$"),
                CheckPatternAfterDownloadInseeCog(colname="end_event_uri", pattern=r"^(http://id.insee.fr/geo/evenementGeographique/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12})?$"),
                CheckEventsUnequalAfterDownloadInseeCog(),
                CheckStartDateAfterDownloadInseeCog(),
                CheckEndDateAfterDownloadInseeCog(),
                CheckEndEventConsistencyAfterDownloadInseeCog(),
                CheckDateConsistencyAfterDownloadInseeCog(),
                CheckInseeCodeOverlapAfterDownloadInseeCog()
            ]
        )
       

class RequestCOGArrondissementMunicipal(RequestCOG):
    """Class to query all municipal arrondissements from the COG"""
    def __init__(
            self,
            output_paths: OutputPathsRequestCOG,
            acquisition_config: InseeSupplierConfig = InseeSupplierConfig(),
            exceptions_handler_config: ArrondissementsMunicipauxInseeExceptionsToIgnoreOrCorrect = ArrondissementsMunicipauxInseeExceptionsToIgnoreOrCorrect()
        ):
        super().__init__(
            output_paths=output_paths,
            request=Path(__file__).parent / "requests" /  "arrondissements_municipaux.rq",
            description='"Arrondissements municipaux" data',
            view_name="insee_arrondissements_municipaux",
            exceptions_handler_config=exceptions_handler_config,
            acquisition_config=acquisition_config,
            sql_templates= TemplatesSQLRequestCOG(
                copy=Path(__file__).parent / "sql" / "arrondissements_municipaux_copy.mustache.sql",
                create_view=Path(__file__).parent / "sql" / "arrondissements_municipaux_import.mustache.sql",
                update=Path(__file__).parent / "sql" / "arrondissements_municipaux_correct.mustache.sql"
            ),
            colnames=[
                'uri',
                'insee_code',
                'label',
                'article_code',
                'parent_uri',
                'start_event_uri',
                'end_event_uri',
                'start_date',
                'end_date',
                'parent_uri_count',
                'start_date_count',
                'end_date_count'
            ],
            extra_controls = [
                CheckPatternAfterDownloadInseeCog(colname="uri", pattern=r"^http://id.insee.fr/geo/arrondissementMunicipal/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$"),
                CheckURIUnicityAfterDownloadInseeCog(),
                CheckPatternAfterDownloadInseeCog(colname="insee_code", pattern=r"^(13|69|75)[0-9]{3}$"),
                CheckPatternAfterDownloadInseeCog(colname="article_code", pattern=r"^[0-8X]$"),
                CheckPatternAfterDownloadInseeCog(colname="parent_uri", pattern=r"^(http://id.insee.fr/geo/commune/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12})([|]http://id.insee.fr/geo/commune/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12})*$"),
                CheckPatternAfterDownloadInseeCog(colname="start_event_uri", pattern=r"^http://id.insee.fr/geo/evenementGeographique/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$"),
                CheckPatternAfterDownloadInseeCog(colname="end_event_uri", pattern=r"^(http://id.insee.fr/geo/evenementGeographique/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12})?$"),
                CheckEventsUnequalAfterDownloadInseeCog(),
                CheckStartDateAfterDownloadInseeCog(),
                CheckEndDateAfterDownloadInseeCog(),
                CheckEndEventConsistencyAfterDownloadInseeCog(),
                CheckDateConsistencyAfterDownloadInseeCog(),
                CheckInseeCodeOverlapAfterDownloadInseeCog()
            ]
        )

class RequestCOGDepartement(RequestCOG):
    """Class to query all departments from the COG"""
    def __init__(
            self,
            output_paths: OutputPathsRequestCOG,
            acquisition_config: InseeSupplierConfig = InseeSupplierConfig(),
            exceptions_handler_config: DepartementsInseeExceptionsToIgnoreOrCorrect = DepartementsInseeExceptionsToIgnoreOrCorrect()
        ):
        super().__init__(
            output_paths=output_paths,
            request=Path(__file__).parent / "requests" /  "departements.rq",
            description='"Departements" data',
            view_name="insee_departements",
            exceptions_handler_config=exceptions_handler_config,
            acquisition_config=acquisition_config,
            sql_templates= TemplatesSQLRequestCOG(
                copy=Path(__file__).parent / "sql" / "departements_copy.mustache.sql",
                create_view=Path(__file__).parent / "sql" / "departements_import.mustache.sql",
                update=Path(__file__).parent / "sql" / "departements_correct.mustache.sql"
            ),
            colnames=[
                'uri',
                'insee_code',
                'label',
                'article_code',
                'start_event_uri',
                'end_event_uri',
                'start_date',
                'end_date',
                'start_date_count',
                'end_date_count'
            ],
            extra_controls = [
                CheckPatternAfterDownloadInseeCog(colname="uri", pattern=r"^http://id.insee.fr/geo/departement/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$"),
                CheckURIUnicityAfterDownloadInseeCog(),
                CheckPatternAfterDownloadInseeCog(colname="insee_code", pattern=r"^(0[1-9]|[1-8][0-9]|9[0-5]|2[AB]|97[1-9])$"),
                CheckPatternAfterDownloadInseeCog(colname="article_code", pattern=r"^[0-8X]$"),
                CheckPatternAfterDownloadInseeCog(colname="start_event_uri", pattern=r"^http://id.insee.fr/geo/evenementGeographique/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$"),
                CheckPatternAfterDownloadInseeCog(colname="end_event_uri", pattern=r"^(http://id.insee.fr/geo/evenementGeographique/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12})?$"),
                CheckEventsUnequalAfterDownloadInseeCog(),
                CheckStartDateAfterDownloadInseeCog(),
                CheckEndDateAfterDownloadInseeCog(),
                CheckEndEventConsistencyAfterDownloadInseeCog(),
                CheckDateConsistencyAfterDownloadInseeCog(),
                CheckInseeCodeOverlapAfterDownloadInseeCog()
            ]
        )



class RequestsCOGDistrict(RequestCOG):
    """Class to query all districts from the COG"""
    def __init__(
            self,
            output_paths: OutputPathsRequestCOG,
            acquisition_config: InseeSupplierConfig = InseeSupplierConfig(),
            exceptions_handler_config: DistrictsInseeExceptionsToIgnoreOrCorrect = DistrictsInseeExceptionsToIgnoreOrCorrect()
        ):
        super().__init__(
            output_paths=output_paths,
            request=Path(__file__).parent / "requests" /  "districts.rq",
            description='"Districts" data',
            view_name="insee_districts",
            exceptions_handler_config=exceptions_handler_config,
            acquisition_config=acquisition_config,
            sql_templates= TemplatesSQLRequestCOG(
                copy=Path(__file__).parent / "sql" / "districts_copy.mustache.sql",
                create_view=Path(__file__).parent / "sql" / "districts_import.mustache.sql",
                update=Path(__file__).parent / "sql" / "districts_correct.mustache.sql"
            ),
            colnames=[
                'uri',
                'insee_code',
                'label',
                'article_code',
                'start_event_uri',
                'end_event_uri',
                'start_date',
                'end_date',
                'start_date_count',
                'end_date_count'
            ],
            extra_controls = [
                CheckPatternAfterDownloadInseeCog(colname="uri", pattern=r"^http://id.insee.fr/geo/district/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$"),
                CheckURIUnicityAfterDownloadInseeCog(),
                CheckPatternAfterDownloadInseeCog(colname="insee_code", pattern=r"^98[0-9]{3}$"),
                CheckPatternAfterDownloadInseeCog(colname="article_code", pattern=r"^[0-8X]$"),
                CheckPatternAfterDownloadInseeCog(colname="start_event_uri", pattern=r"^http://id.insee.fr/geo/evenementGeographique/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$"),
                CheckPatternAfterDownloadInseeCog(colname="end_event_uri", pattern=r"^(http://id.insee.fr/geo/evenementGeographique/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12})?$"),
                CheckEventsUnequalAfterDownloadInseeCog(),
                CheckStartDateAfterDownloadInseeCog(),
                CheckEndDateAfterDownloadInseeCog(),
                CheckEndEventConsistencyAfterDownloadInseeCog(),
                CheckDateConsistencyAfterDownloadInseeCog(),
                CheckInseeCodeOverlapAfterDownloadInseeCog()
            ]
        )

        

class RequestsCOGCollectivitesOutremer(RequestCOG):
    """Class to query all Overseas collectivity from the COG"""
    def __init__(
            self,
            output_paths: OutputPathsRequestCOG,
            acquisition_config: InseeSupplierConfig = InseeSupplierConfig(),
            exceptions_handler_config: CollectivitesDOutreMerInseeExceptionsToIgnoreOrCorrect = CollectivitesDOutreMerInseeExceptionsToIgnoreOrCorrect()
        ):
        super().__init__(
            output_paths=output_paths,
            request=Path(__file__).parent / "requests" /  "collectivites_outremer.rq",
            description='"Collectivités d\'Outre-mer" (i.e. Overseas collectivity) data',
            view_name="insee_collectivites_outremer",
            exceptions_handler_config=exceptions_handler_config,
            acquisition_config=acquisition_config,
            sql_templates= TemplatesSQLRequestCOG(
                copy=Path(__file__).parent / "sql" / "collectivites_outremer_copy.mustache.sql",
                create_view=Path(__file__).parent / "sql" / "collectivites_outremer_import.mustache.sql",
                update=Path(__file__).parent / "sql" / "collectivites_outremer_correct.mustache.sql"
            ),
            colnames=[
                'uri',
                'insee_code',
                'label',
                'article_code',
                'start_event_uri',
                'end_event_uri',
                'start_date',
                'end_date',
                'start_date_count',
                'end_date_count'
            ],
            extra_controls = [
                CheckPatternAfterDownloadInseeCog(colname="uri", pattern=r"^http://id.insee.fr/geo/collectiviteDOutreMer/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$"),
                CheckURIUnicityAfterDownloadInseeCog(),
                CheckPatternAfterDownloadInseeCog(colname="insee_code", pattern=r"^(95|96|975|976|977|978|981|984|985|986|987|988|989|98[0-9]{3})$"),
                CheckPatternAfterDownloadInseeCog(colname="article_code", pattern=r"^[0-8X]$"),
                CheckPatternAfterDownloadInseeCog(colname="start_event_uri", pattern=r"^http://id.insee.fr/geo/evenementGeographique/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$"),
                CheckPatternAfterDownloadInseeCog(colname="end_event_uri", pattern=r"^(http://id.insee.fr/geo/evenementGeographique/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12})?$"),
                CheckEventsUnequalAfterDownloadInseeCog(),
                CheckStartDateAfterDownloadInseeCog(),
                CheckEndDateAfterDownloadInseeCog(),
                CheckEndEventConsistencyAfterDownloadInseeCog(),
                CheckDateConsistencyAfterDownloadInseeCog(),
                CheckInseeCodeOverlapAfterDownloadInseeCog()                
            ]
        )

class RequestsCOGPays(RequestCOG):
    """Class to query all countries from the COG"""
    def __init__(
            self,
            output_paths: OutputPathsRequestCOG,
            acquisition_config: InseeSupplierConfig = InseeSupplierConfig(),
            exceptions_handler_config: PaysInseeExceptionsToIgnoreOrCorrect = PaysInseeExceptionsToIgnoreOrCorrect()
        ):
        super().__init__(
            output_paths=output_paths,
            request=Path(__file__).parent / "requests" / "pays.rq",
            description='"Pays" (i.e. country) data',
            view_name="insee_pays",
            exceptions_handler_config=exceptions_handler_config,
            acquisition_config=acquisition_config,
            sql_templates= TemplatesSQLRequestCOG(
                copy=Path(__file__).parent / "sql" / "pays_copy.mustache.sql",
                create_view=Path(__file__).parent / "sql" / "pays_import.mustache.sql",
                update=Path(__file__).parent / "sql" / "pays_correct.mustache.sql"
            ),
            colnames=[
                'uri',
                'insee_code',
                'label',
                'article_code',
                'long_label',
                'iso3166alpha2_code',
                'iso3166alpha3_code',
                'iso3166num_code',
                'start_event_uri',
                'end_event_uri',
                'start_date',
                'end_date',
                'start_date_count',
                'end_date_count'
            ],
            extra_controls = [
                CheckPatternAfterDownloadInseeCog(colname="uri", pattern=r"^http://id.insee.fr/geo/pays/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$"),
                CheckURIUnicityAfterDownloadInseeCog(),
                CheckPatternAfterDownloadInseeCog(colname="insee_code", pattern=r"^99[0-9]{3}$"),
                CheckPatternAfterDownloadInseeCog(colname="article_code", pattern=r"^[0-8X]$"),
                CheckPatternAfterDownloadInseeCog(colname="iso3166alpha2_code", pattern=r"^([A-Z]{2})?$"),
                CheckPatternAfterDownloadInseeCog(colname="iso3166alpha3_code", pattern=r"^([A-Z]{3})?$"),
                CheckPatternAfterDownloadInseeCog(colname="iso3166num_code", pattern=r"^([0-9]{3})?$"),
                CheckPatternAfterDownloadInseeCog(colname="start_event_uri", pattern=r"^http://id.insee.fr/geo/evenementGeographique/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$"),
                CheckPatternAfterDownloadInseeCog(colname="end_event_uri", pattern=r"^(http://id.insee.fr/geo/evenementGeographique/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12})?$"),
                CheckEventsUnequalAfterDownloadInseeCog(),
                CheckStartDateAfterDownloadInseeCog(),
                CheckEndDateAfterDownloadInseeCog(),
                CheckEndEventConsistencyAfterDownloadInseeCog(),
                CheckDateConsistencyAfterDownloadInseeCog(),
                CheckInseeCodeOverlapAfterDownloadInseeCog()
            ]
        )
