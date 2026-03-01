from pathlib import Path
from typing import Union
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from duckdb import DuckDBPyConnection
import logging
import pystache
import csv

from .config import LaPosteExceptionsToIgnoreOrCorrect, LaPosteSupplierConfig
from .checks.abstract import DataValidationAndConsistencyLaPosteHexasmal
from .checks.parsing import CheckParsingAfterDownloadLaPosteHexasmal
from .checks.pattern import CheckPatternAfterDownloadLaPosteHexasmal

class TemplatesSQLRequestLaPosteHexasmal:
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

class OutputPathsRequestLaPosteHexasmal:
    def __init__(
            self,
            raw_entities: Union[str, Path],
            add_entities: Union[str, Path],
            remove_entities: Union[str, Path],
            cleaned_entities: Union[str, Path]
        ):
        if isinstance(raw_entities, str):
            self.raw_entities = Path(raw_entities)
        else:
            self.raw_entities = raw_entities
        if isinstance(add_entities, str):
            self.add_entities = Path(add_entities)
        else:
            self.add_entities = add_entities
        if isinstance(remove_entities, str):
            self.remove_entities = Path(remove_entities)
        else:
            self.remove_entities = remove_entities
        if isinstance(cleaned_entities, str):
            self.cleaned_entities = Path(cleaned_entities)
        else:
            self.cleaned_entities = cleaned_entities

class RequestLaPosteHexasmal:
    """Class to load hexasmal base of La Poste"""
    def __init__(
            self,
            output_paths: OutputPathsRequestLaPosteHexasmal,
            exceptions_handler_config: LaPosteExceptionsToIgnoreOrCorrect,
            acquisition_config: LaPosteSupplierConfig = LaPosteSupplierConfig()
        ):
        self.output_paths = output_paths
        self.view_name = "laposte_hexasmal"
        self.exceptions_handler_config = exceptions_handler_config
        self.sql_templates = TemplatesSQLRequestLaPosteHexasmal(
            copy=Path(__file__).parent / "sql" / "laposte_hexasmal_copy.mustache.sql",
            create_view=Path(__file__).parent / "sql" / "laposte_hexasmal_import.mustache.sql",
            update=Path(__file__).parent / "sql" / "laposte_hexasmal_correct.mustache.sql"
        )
        self.acquisition_config = acquisition_config
        self.extra_controls = [
            CheckPatternAfterDownloadLaPosteHexasmal(colname="insee_code", pattern=r"^((0[1-9]|[1-8][0-9]|9[0-8]|2[AB])[0-9]{3}|99138)$"),
            CheckPatternAfterDownloadLaPosteHexasmal(colname="postal_code", pattern=r"^[0-9]{5}$")
        ]

    def send(self) -> None:
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
            with session.get(
                url=self.acquisition_config.endpoint_url,
                data=None,
                timeout=(self.acquisition_config.read_timeout, self.acquisition_config.connect_timeout)
            ) as response:
                if response.status_code != 200:
                    raise requests.exceptions.HTTPError(f"HTTP error while querying La Poste: {response.status_code}")
                
                if not self.output_paths.raw_entities.parent.exists():
                    self.output_paths.raw_entities.parent.mkdir(parents=True, exist_ok=True)
                if self.output_paths.raw_entities.exists():
                   self.output_paths.raw_entities.unlink()

                with open(self.output_paths.raw_entities, "wb") as foutput:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            foutput.write(chunk)


        except requests.exceptions.Timeout as e:
            raise TimeoutError(f"Timeout occurred while querying La Poste") from e
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Connection error while querying La Poste") from e
        except requests.exceptions.HTTPError as e:
            raise e
        except requests.exceptions.RequestException  as e:
            raise RuntimeError(f"Request error while querying La Poste") from e
        except Exception as e:
            raise RuntimeError(f"Unexpected error while querying La Poste") from e
        
    def apply_updates(self, duckdb_conn: DuckDBPyConnection):
        """Apply updates before checks"""
        if not self.output_paths.add_entities.parent.exists():
            self.output_paths.add_entities.parent.mkdir(parents=True, exist_ok=True)
        if self.output_paths.add_entities.exists():
            self.output_paths.add_entities.unlink()
        
        with open(self.output_paths.add_entities, mode="w", newline="", encoding="utf-8") as f_add:
            fieldnames_add = ['insee_code', 'label', 'postal_code', 'delivery_label', 'associated_name']
            writer_add = csv.DictWriter(f_add, fieldnames=fieldnames_add)
            writer_add.writeheader()
            for key, list_add in self.exceptions_handler_config.root.items():
                for el in list_add:
                    writer_add.writerow(
                        {
                            "insee_code": key,
                            "label": el.name,
                            "postal_code": el.postal_code,
                            "delivery_label": el.delivery_label,
                            "associated_name": el.associated_name
                        }
                    )
       
        if not self.output_paths.remove_entities.parent.exists():
            self.output_paths.remove_entities.parent.mkdir(parents=True, exist_ok=True)
        if self.output_paths.remove_entities.exists():
            self.output_paths.remove_entities.unlink()

        with open(self.output_paths.remove_entities, mode="w", newline="", encoding="utf-8") as f_remove:
            fieldnames_remove = ['insee_code']
            writer_remove = csv.DictWriter(f_remove, fieldnames=fieldnames_remove)
            writer_remove.writeheader()
            for key in self.exceptions_handler_config.root.keys():
                writer_remove.writerow({'insee_code': key})
        
        output_path_tmp =  self.output_paths.cleaned_entities.with_suffix(".tmp")
        if not self.output_paths.cleaned_entities.parent.exists():
            self.output_paths.cleaned_entities.parent.mkdir(parents=True, exist_ok=True)
        if output_path_tmp.exists():
            output_path_tmp.unlink()

        context_apply_updates: dict[str, str] = {
            "view_name": self.view_name,
            "path_add": str(self.output_paths.add_entities.resolve()),
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
        logging.info(f"Checking content of La Poste Hexasmal data after downloading")
        controls: list[DataValidationAndConsistencyLaPosteHexasmal] = [CheckParsingAfterDownloadLaPosteHexasmal()]
        controls.extend(self.extra_controls)
        nb_controls = len(controls)
        if nb_controls == 0:
            logging.info(f"No control to run for La Poste Hexasmal data after downloading")
        for current_step, control in enumerate(controls):
            logging.info(f"Running check {current_step+1}/{nb_controls}: {type(control).__name__}")
            control.run(request=self, duckdb_conn=duckdb_conn)
        logging.info(f"All checks passed for La Poste Hexasmal data after downloading")
