from pathlib import Path
from typing import Union, Optional
from abc import ABC, abstractmethod
from urllib.parse import quote_plus
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from duckdb import DuckDBPyConnection
import logging

from .config import InseeSupplierConfig, InseeExceptionsToIgnoreOrCorrectModel, CommunesInseeExceptionsToIgnoreOrCorrect, ArrondissementsMunicipauxInseeExceptionsToIgnoreOrCorrect, CollectivitesDOutreMerInseeExceptionsToIgnoreOrCorrect, DistrictsInseeExceptionsToIgnoreOrCorrect, PaysInseeExceptionsToIgnoreOrCorrect, EventsInseeExceptionsToIgnoreOrCorrect
from .checks.abstract import DataValidationAndConsistencyInseeCog
from .checks.article_code import CheckArticleCodeAfterDownloadInseeCog
from .checks.date_consistency import CheckDateConsistencyAfterDownloadInseeCog
from .checks.insee_code import CheckInseeCodeAfterDownloadInseeCog
from .checks.insee_code_overlap import CheckInseeCodeOverlapAfterDownloadInseeCog
from .checks.parsing import CheckParsingAfterDownloadInseeCog
from .checks.start_date import CheckStartDateAfterDownloadInseeCog
from .checks.end_date import CheckEndDateAfterDownloadInseeCog
from .checks.uri_format import CheckURIAfterDownloadInseeCog
from .checks.uri_unicity import CheckURIUnicityAfterDownloadInseeCog


class RequestCOG(ABC):
    """Abstract base class for querying the Official geographic code alias COG (Code officiel géographique)"""
    headers = {"Content-type": "application/x-www-form-urlencoded"}

    def __init__(
            self,
            output_path_before_checks: Union[str, Path],
            output_path_after_checks: Union[str, Path],
            request: Union[str, Path],
            description: str,
            view_name: str,
            exceptions_handler_config: InseeExceptionsToIgnoreOrCorrectModel,
            template_copy_path: Union[str, Path],
            template_import_path: Union[str, Path],
            acquisition_config: InseeSupplierConfig = InseeSupplierConfig(),
            controls: list[DataValidationAndConsistencyInseeCog] = []
            
        ):
        self.output_path_before_checks = output_path_before_checks
        self.output_path_after_checks = output_path_after_checks    
        self.request = request
        self.description =description
        self.view_name = view_name
        self.exceptions_handler_config = exceptions_handler_config
        self.acquisition_config = acquisition_config
        self.template_copy_path = template_copy_path
        self.template_import_path = template_import_path
        self.controls = controls

    def send(self):
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
                
                if isinstance(self.output_path_before_checks, str):
                    self.output_path = Path(self.output_path_before_checks)
                if not self.output_path_before_checks.parent.exists():
                    self.output_path_before_checks.parent.mkdir(parents=True, exist_ok=True)
                if self.output_path_before_checks.exists():
                   self.output_path_before_checks.unlink()

                with open(self.output_path_before_checks, "wb") as foutput:
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
        
    def check_content(self, duckdb_conn : DuckDBPyConnection) -> bool:
        """Check if the content of the file is valid"""
        logging.info(f"Checking content of {self.description} after downloading")
        nb_controls = len(self.controls)
        if nb_controls == 0:
            logging.info(f"No control to run for {self.description} after downloading")
            return True
        
        last_step : int = -1
        current_step : int = 0

        while current_step < nb_controls:
            control = self.controls[current_step]
            logging.info(f"Running check {current_step+1}/{nb_controls}: {type(control).__name__}")
            if last_step < current_step:
                check = control.run(request=self, duckdb_conn=duckdb_conn, allow_exceptions=True)
                if not check:
                    logging.warning(f"Check {current_step+1}/{nb_controls} failed for {self.description}. Controls are reset after handling exception and will be re-run from the first one.")
                    last_step = current_step
                    current_step = 0
                else:
                    last_step = current_step
                    current_step += 1
            else:
                check = control.run(request=self, duckdb_conn=duckdb_conn, allow_exceptions=True)
                if not check:
                    check_rerrun = control.run(request=self, duckdb_conn=duckdb_conn, allow_exceptions=False)
                    if not check_rerrun:
                        raise RuntimeError(f"Unexpected error in the control flow of checks for {self.description} after downloading. Check {current_step+1}/{nb_controls} should have passed after handling exceptions but still fails after re-running the check without allowing exceptions.")
                current_step += 1
        logging.info(f"All checks passed for {self.description} after downloading")

class RequestCOGCommune(RequestCOG):
    """Class to query all communes from the COG"""
    def __init__(
            self,
            output_path_before_checks: Union[str, Path],
            output_path_after_checks: Union[str, Path],
            acquisition_config: InseeSupplierConfig = InseeSupplierConfig(),
            exceptions_handler_config: CommunesInseeExceptionsToIgnoreOrCorrect = CommunesInseeExceptionsToIgnoreOrCorrect()
        ):
        super().__init__(
            output_path_before_checks=output_path_before_checks,
            output_path_after_checks=output_path_after_checks,
            request=Path(__file__).parent / "requests" / "communes.rq",
            description='"Communes" data',
            view_name="insee_communes",
            exceptions_handler_config=exceptions_handler_config,
            acquisition_config=acquisition_config,
            template_import_path=Path(__file__).parent / "sql" / "communes_import.mustache.sql",
            template_copy_path=Path(__file__).parent / "sql" / "communes_copy.mustache.sql",
            controls = [
                CheckParsingAfterDownloadInseeCog(),
                CheckURIAfterDownloadInseeCog(pattern=r"^http://id.insee.fr/geo/commune/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$", uri_format_exceptions= exceptions_handler_config.uri_format),
                CheckURIUnicityAfterDownloadInseeCog(uri_unicity_exceptions= exceptions_handler_config.uri_unicity),
                CheckInseeCodeAfterDownloadInseeCog(pattern=r"^(0[1-9]|[1-9][0-9]|9[0-8]|2[AB)])[0-9]{3}$", insee_code_exceptions=exceptions_handler_config.insee_code),
                CheckArticleCodeAfterDownloadInseeCog(article_code_exceptions=exceptions_handler_config.article_code),
                CheckStartDateAfterDownloadInseeCog(start_date_exceptions=exceptions_handler_config.start_date),
                CheckEndDateAfterDownloadInseeCog(end_date_exceptions=exceptions_handler_config.end_date),
                CheckDateConsistencyAfterDownloadInseeCog(date_consistency_exceptions=exceptions_handler_config.date_consistency),
                CheckInseeCodeOverlapAfterDownloadInseeCog(insee_code_overlap_exceptions=exceptions_handler_config.insee_code_overlap)
            ]
        )


class RequestCOGArrondissementMunicipal(RequestCOG):
    """Class to query all municipal arrondissements from the COG"""
    def __init__(
            self,
            output_path_before_checks: Union[str, Path],
            output_path_after_checks: Union[str, Path],
            acquisition_config: InseeSupplierConfig = InseeSupplierConfig(),
            exceptions_handler_config: ArrondissementsMunicipauxInseeExceptionsToIgnoreOrCorrect = ArrondissementsMunicipauxInseeExceptionsToIgnoreOrCorrect()
        ):
        super().__init__(
            output_path_before_checks=output_path_before_checks,
            output_path_after_checks=output_path_after_checks,
            request=Path(__file__).parent / "requests" /  "arrondissements_municipaux.rq",
            description='"Arrondissements municipaux" data',
            view_name="insee_arrondissements_municipaux",
            exceptions_handler_config=exceptions_handler_config,
            acquisition_config=acquisition_config,
            template_import_path=Path(__file__).parent / "sql" / "arrondissements_municipaux_import.mustache.sql",
            template_copy_path=Path(__file__).parent / "sql" / "arrondissements_municipaux_copy.mustache.sql",
            controls = [
                CheckParsingAfterDownloadInseeCog(),
                CheckURIAfterDownloadInseeCog(pattern=r"^http://id.insee.fr/geo/arrondissementMunicipal/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$", uri_format_exceptions= exceptions_handler_config.uri_format),
                CheckURIUnicityAfterDownloadInseeCog(uri_unicity_exceptions= exceptions_handler_config.uri_unicity),
                CheckInseeCodeAfterDownloadInseeCog(pattern=r"^(13|69|75)[0-9]{3}$", insee_code_exceptions=exceptions_handler_config.insee_code),
                CheckArticleCodeAfterDownloadInseeCog(article_code_exceptions=exceptions_handler_config.article_code),
                CheckStartDateAfterDownloadInseeCog(start_date_exceptions=exceptions_handler_config.start_date),
                CheckEndDateAfterDownloadInseeCog(end_date_exceptions=exceptions_handler_config.end_date),
                CheckDateConsistencyAfterDownloadInseeCog(date_consistency_exceptions=exceptions_handler_config.date_consistency),
                CheckInseeCodeOverlapAfterDownloadInseeCog(insee_code_overlap_exceptions=exceptions_handler_config.insee_code_overlap)
            ]
        )


class RequestsCOGDistrict(RequestCOG):
    """Class to query all districts from the COG"""
    def __init__(
            self,
            output_path_before_checks: Union[str, Path],
            output_path_after_checks: Union[str, Path],
            acquisition_config: InseeSupplierConfig = InseeSupplierConfig(),
            exceptions_handler_config: DistrictsInseeExceptionsToIgnoreOrCorrect = DistrictsInseeExceptionsToIgnoreOrCorrect()
        ):
        super().__init__(
            output_path_before_checks=output_path_before_checks,
            output_path_after_checks=output_path_after_checks,
            request=Path(__file__).parent / "requests" /  "districts.rq",
            description='"Districts" data',
            view_name="insee_districts",
            exceptions_handler_config=exceptions_handler_config,
            acquisition_config=acquisition_config,
            template_import_path=Path(__file__).parent / "sql" / "districts_import.mustache.sql",
            template_copy_path=Path(__file__).parent / "sql" / "districts_copy.mustache.sql",
            controls = [
                CheckParsingAfterDownloadInseeCog(),
                CheckURIAfterDownloadInseeCog(pattern=r"^http://id.insee.fr/geo/district/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$", uri_format_exceptions= exceptions_handler_config.uri_format),
                CheckURIUnicityAfterDownloadInseeCog(uri_unicity_exceptions= exceptions_handler_config.uri_unicity),
                CheckInseeCodeAfterDownloadInseeCog(pattern=r"^98[0-9]{3}$", insee_code_exceptions=exceptions_handler_config.insee_code),
                CheckArticleCodeAfterDownloadInseeCog(article_code_exceptions=exceptions_handler_config.article_code),
                CheckStartDateAfterDownloadInseeCog(start_date_exceptions=exceptions_handler_config.start_date),
                CheckEndDateAfterDownloadInseeCog(end_date_exceptions=exceptions_handler_config.end_date),
                CheckDateConsistencyAfterDownloadInseeCog(date_consistency_exceptions=exceptions_handler_config.date_consistency),
                CheckInseeCodeOverlapAfterDownloadInseeCog(insee_code_overlap_exceptions=exceptions_handler_config.insee_code_overlap)
            ]
        )

        

class RequestsCOGCollectivitesOutremer(RequestCOG):
    """Class to query all Overseas collectivity from the COG"""
    def __init__(
            self,
            output_path_before_checks: Union[str, Path],
            output_path_after_checks: Union[str, Path],
            acquisition_config: InseeSupplierConfig = InseeSupplierConfig(),
            exceptions_handler_config: CollectivitesDOutreMerInseeExceptionsToIgnoreOrCorrect = CollectivitesDOutreMerInseeExceptionsToIgnoreOrCorrect()
        ):
        super().__init__(
            output_path_before_checks=output_path_before_checks,
            output_path_after_checks=output_path_after_checks,
            request=Path(__file__).parent / "requests" /  "collectivites_outremer.rq",
            description='"Collectivités d\'Outre-mer" (i.e. Overseas collectivity) data',
            view_name="insee_collectivites_outremer",
            exceptions_handler_config=exceptions_handler_config,
            acquisition_config=acquisition_config,
            template_import_path=Path(__file__).parent / "sql" / "collectivites_outremer_import.mustache.sql",
            template_copy_path=Path(__file__).parent / "sql" / "collectivites_outremer_copy.mustache.sql",
            controls = [
                CheckParsingAfterDownloadInseeCog(),
                CheckURIAfterDownloadInseeCog(pattern=r"^http://id.insee.fr/geo/collectiviteDOutreMer/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$", uri_format_exceptions= exceptions_handler_config.uri_format),
                CheckURIUnicityAfterDownloadInseeCog(uri_unicity_exceptions= exceptions_handler_config.uri_unicity),
                CheckInseeCodeAfterDownloadInseeCog(pattern=r"^(95|96|975|976|977|978|981|984|985|986|987|988|989|98[0-9]{3})$", insee_code_exceptions=exceptions_handler_config.insee_code),
                CheckArticleCodeAfterDownloadInseeCog(article_code_exceptions=exceptions_handler_config.article_code),
                CheckStartDateAfterDownloadInseeCog(start_date_exceptions=exceptions_handler_config.start_date),
                CheckEndDateAfterDownloadInseeCog(end_date_exceptions=exceptions_handler_config.end_date),
                CheckDateConsistencyAfterDownloadInseeCog(date_consistency_exceptions=exceptions_handler_config.date_consistency),
                CheckInseeCodeOverlapAfterDownloadInseeCog(insee_code_overlap_exceptions=exceptions_handler_config.insee_code_overlap)
            ]
        )

class RequestsCOGPays(RequestCOG):
    """Class to query all countries from the COG"""
    def __init__(
            self,
            output_path_before_checks: Union[str, Path],
            output_path_after_checks: Union[str, Path],
            acquisition_config: InseeSupplierConfig = InseeSupplierConfig(),
            exceptions_handler_config: PaysInseeExceptionsToIgnoreOrCorrect = PaysInseeExceptionsToIgnoreOrCorrect()
        ):
        super().__init__(
            output_path_before_checks=output_path_before_checks,
            output_path_after_checks=output_path_after_checks,
            request=Path(__file__).parent / "requests" / "pays.rq",
            description='"Pays" (i.e. country) data',
            view_name="insee_pays",
            exceptions_handler_config=exceptions_handler_config,
            acquisition_config=acquisition_config,
            template_import_path=Path(__file__).parent / "sql" / "pays_import.mustache.sql",
            template_copy_path=Path(__file__).parent / "sql" / "pays_copy.mustache.sql",
            controls = [
                CheckParsingAfterDownloadInseeCog(),
                CheckURIAfterDownloadInseeCog(pattern=r"^http://id.insee.fr/geo/pays/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$", uri_format_exceptions= exceptions_handler_config.uri_format),
                CheckURIUnicityAfterDownloadInseeCog(uri_unicity_exceptions= exceptions_handler_config.uri_unicity),
                CheckInseeCodeAfterDownloadInseeCog(pattern=r"^99[0-9]{3}$", insee_code_exceptions=exceptions_handler_config.insee_code),
                CheckArticleCodeAfterDownloadInseeCog(article_code_exceptions=exceptions_handler_config.article_code),
                CheckStartDateAfterDownloadInseeCog(start_date_exceptions=exceptions_handler_config.start_date),
                CheckEndDateAfterDownloadInseeCog(end_date_exceptions=exceptions_handler_config.end_date),
                CheckDateConsistencyAfterDownloadInseeCog(date_consistency_exceptions=exceptions_handler_config.date_consistency),
                CheckInseeCodeOverlapAfterDownloadInseeCog(insee_code_overlap_exceptions=exceptions_handler_config.insee_code_overlap)
            ]
        )
        

class RequestsCOGEvents(RequestCOG):
    """Class to query all events from the COG"""
    def __init__(
            self,
            output_path_before_checks: Union[str, Path],
            output_path_after_checks: Union[str, Path],
            acquisition_config: InseeSupplierConfig = InseeSupplierConfig(),
            exceptions_handler_config: EventsInseeExceptionsToIgnoreOrCorrect = EventsInseeExceptionsToIgnoreOrCorrect()
        ):
        super().__init__(
            output_path_before_checks=output_path_before_checks,
            output_path_after_checks=output_path_after_checks,
            request=Path(__file__).parent / "requests" / "events.rq",
            description='events data',
            view_name = "insee_events",
            exceptions_handler_config=exceptions_handler_config,
            acquisition_config=acquisition_config,
            template_import_path=Path(__file__).parent / "sql" / "events_import.mustache.sql",
            template_copy_path=Path(__file__).parent / "sql" / "events_copy.mustache.sql"
        )

    def check_content(self, duckdb_conn : DuckDBPyConnection) -> bool:
        """Check if the content of the file is valid"""
        pass
        