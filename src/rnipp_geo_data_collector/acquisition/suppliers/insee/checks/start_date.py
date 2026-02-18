from __future__ import annotations

from pathlib import Path
import logging
import pystache
from duckdb import DuckDBPyConnection
from typing import TYPE_CHECKING
import datetime

from .abstract import DataValidationAndConsistencyInseeCog
from .error_handler import ErrorHandlerReplaceBadStartDateAfterDownloadInseeCog, ErrorHandlerRemoveBadStartDateAfterDownloadInseeCog

if TYPE_CHECKING:
    from ..requests import RequestCOG
    from ..config import StartDateInseeExceptionsToIgnoreOrCorrect


class CheckStartDateAfterDownloadInseeCog(DataValidationAndConsistencyInseeCog):
    def __init__(
            self,
            start_date_exceptions: StartDateInseeExceptionsToIgnoreOrCorrect
        ):
        super().__init__()
        self.start_date_exceptions = start_date_exceptions

    def handle_exceptions(self, request: RequestCOG, duckdb_conn: DuckDBPyConnection):
        """Handle exceptions in start date checks"""
        template_path = Path(__file__).parent.parent / "sql" / "start_date_correct.mustashe.sql"
        renderer = pystache.Renderer(escape=lambda s: s) 
        
        modif_start_date_sql = "start_date"
        filter_condition = ""

        uri_updates = [exception for exception in self.start_date_exceptions.root if isinstance(exception, ErrorHandlerReplaceBadStartDateAfterDownloadInseeCog)]
        uri_removals = [exception for exception in self.start_date_exceptions.root if isinstance(exception, ErrorHandlerRemoveBadStartDateAfterDownloadInseeCog)]
        
        if uri_updates:
            modif_start_date_sql = "CASE " + " ".join([f"WHEN uri = '{exception.uri}' THEN '{exception.start_date}'::DATE" for exception in uri_updates]) + f" ELSE start_date END"
            
        if uri_removals:
            filter_condition = "WHERE uri NOT IN (" + ", ".join([f"'{exception.uri}'" for exception in uri_removals]) + ")"

        output_path: Path = request.output_path_after_checks if isinstance(request.output_path_after_checks, Path) else Path(request.output_path_after_checks)
        output_path_tmp = Path(output_path).with_suffix(".tmp")
    
        
        context: dict[str, str] = {
            "modif_start_date": modif_start_date_sql,
            "filter_condition": filter_condition,
            "view_name": request.view_name,
            "output_path": str(output_path_tmp.resolve())
        }

        try:
            with open(template_path, 'r', encoding='utf-8') as template_file:
                template_content = template_file.read()
        except Exception as e:
            raise RuntimeError(f"Failed to load template file {template_path}") from e
        
        try:
            rendered_str = renderer.render(template_content, context)
        except Exception as e:
            raise RuntimeError(f"Failed to render template file {template_path}") from e
        
        try:
            duckdb_conn.execute(rendered_str)
            output_path_tmp.replace(output_path)
        except Exception as e:
            raise RuntimeError(f"Failed to execute SQL script {template_path}") from e

    def run(self, request: RequestCOG, duckdb_conn: DuckDBPyConnection, allow_exceptions: bool = False) -> bool:
        """Check if the content of the file is valid for the start date variable"""
        template_path = Path(__file__).parent.parent / "sql" /  "start_date_check.mustashe.sql"
        renderer = pystache.Renderer(escape=lambda s: s) 
        context: dict[str, str] = {
            "view_name": request.view_name
        }

        try:
            with open(template_path, 'r', encoding='utf-8') as template_file:
                template_content = template_file.read()
        except Exception as e:
            raise RuntimeError(f"Failed to load template file {template_path}") from e
        
        try:
            rendered_str = renderer.render(template_content, context)
        except Exception as e:
            raise RuntimeError(f"Failed to render template file {template_path}") from e
        
        try:
            data_bug = duckdb_conn.sql(rendered_str).fetchall()
            if len(data_bug) > 0:
                row_number_bug = data_bug[0][0]
                uri_bug = data_bug[0][1]
                start_date_bug = data_bug[0][2]
                start_date_count_bug = data_bug[0][3]
                date_category_bug = data_bug[0][4]
                if allow_exceptions and uri_bug in [exception.uri for exception in self.start_date_exceptions.root]:
                    self.handle_exceptions(request=request, duckdb_conn=duckdb_conn)
                    return False
                else:
                    if date_category_bug == 'NULL':
                        raise RuntimeError(f"Failed to load {request.description} after downloading. The file may be corrupted or not in the expected format. The start date is empty at row {row_number_bug} for the URI {uri_bug}")
                    elif date_category_bug == 'BEFORE':
                        raise RuntimeError(f"Failed to load {request.description} after downloading. The file may be corrupted or not in the expected format. The start date {start_date_bug} is older than the minimum date 1943-01-01 at row {row_number_bug} for the URI {uri_bug}")
                    elif date_category_bug == 'AFTER':
                        raise RuntimeError(f"Failed to load {request.description} after downloading. The file may be corrupted or not in the expected format. The start date {start_date_bug} is newer than the maximum date {datetime.datetime.now().strftime('%Y-%m-%d')} at row {row_number_bug} for the URI {uri_bug}")
                    elif date_category_bug == 'MULTIPLE':
                        raise RuntimeError(f"Failed to load {request.description} after downloading. The file may be corrupted or not in the expected format. The start date is duplicated {start_date_count_bug} for the URI {uri_bug}")
                    else:
                        raise RuntimeError(f"Unexpected error while checking start date of {request.description} after downloading")
        except Exception as e:
            raise RuntimeError(f"Unexpected error while checking start date of {request.description} after downloading") from e

        logging.info(f"Successfully checked start date of {request.description} after downloading")
        return True
