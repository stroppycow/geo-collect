from __future__ import annotations


from pathlib import Path
import logging
import pystache
from duckdb import DuckDBPyConnection
from typing import TYPE_CHECKING

from .abstract import DataValidationAndConsistencyInseeCog
from .error_handler import ErrorHandlerRemoveParentDuplicatedAfterDownloadInseeCog, ErrorHandlerReplaceParentDuplicatedAfterDownloadInseeCog

if TYPE_CHECKING:
    from ..requests import RequestCOG
    from ..config import ParentUnicityInseeExceptionsToIgnoreOrCorrect


class CheckParentUnicityAfterDownloadInseeCog(DataValidationAndConsistencyInseeCog):
    def __init__(
            self,
            parent_unicity_exceptions: ParentUnicityInseeExceptionsToIgnoreOrCorrect,
            parent_field_name: str,
            count_field_name: str
        ):
        super().__init__()
        self.parent_unicity_exceptions = parent_unicity_exceptions
        self.parent_field_name = parent_field_name
        self.count_field_name = count_field_name

    def handle_exceptions(self, request: RequestCOG, duckdb_conn: DuckDBPyConnection):
        """Handle exceptions in parent unicity checks"""
        template_path = Path(__file__).parent.parent / "sql" / "parent_unicity_correct.mustashe.sql"
        renderer = pystache.Renderer(escape=lambda s: s) 
        
        parent_field_value = self.parent_field_name 
        filter_condition = ""

        uri_updates = [exception for exception in self.parent_unicity_exceptions.root if isinstance(exception, ErrorHandlerReplaceParentDuplicatedAfterDownloadInseeCog)]
        uri_removals = [exception for exception in self.parent_unicity_exceptions.root if isinstance(exception, ErrorHandlerRemoveParentDuplicatedAfterDownloadInseeCog)]
        
        if uri_updates:
            parent_field_value = "CASE " + " ".join([f"WHEN uri = '{exception.uri}' THEN '{exception.parent_code}'" for exception in uri_updates]) + f" ELSE {self.parent_field_name} END"
            
        if uri_removals:
            filter_condition = "WHERE uri NOT IN (" + ", ".join([f"'{exception.uri}'" for exception in uri_removals]) + ")"

        output_path: Path = request.output_path_after_checks if isinstance(request.output_path_after_checks, Path) else Path(request.output_path_after_checks)
        output_path_tmp = Path(output_path).with_suffix(".tmp")
    
        
        context: dict[str, str] = {
            "parent_field_value": parent_field_value,
            "parent_field_name": self.parent_field_name,
            "count_field_name": self.count_field_name,
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
        """Check if the content of the file is valid for the parent unicity"""
        template_path = Path(__file__).parent.parent / "sql" / "parent_unicity_check.mustashe.sql"
        renderer = pystache.Renderer(escape=lambda s: s) 
        context: dict[str, str] = {
            "parent_field": self.parent_field_name,
            "parent_field_count": self.count_field_name,
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
                row_num_bug = data_bug[0][0]
                uri_bug = data_bug[0][1]
                parent_field_bug = data_bug[0][2]
                parent_field_count_bug = data_bug[0][3]
                parent_unicity_status_bug = data_bug[0][4]

                if allow_exceptions and uri_bug in [exception.uri for exception in self.parent_unicity_exceptions.root]:
                    self.handle_exceptions(request=request, duckdb_conn=duckdb_conn)
                    return False
                else:
                    raise RuntimeError(f"Failed to load {request.description} after downloading. The file may be corrupted or not in the expected format. The parent unicity is not respected for the URI {uri_bug} with {self.parent_field_name} value {parent_field_bug} at row {row_num_bug}. The count of the {self.parent_field_name} value is {parent_field_count_bug} and the parent unicity status is {parent_unicity_status_bug}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error while checking parent unicity of {request.description} after downloading") from e
        
        logging.info(f"Successfully checked parent unicity of {request.description} after downloading")
        return True
