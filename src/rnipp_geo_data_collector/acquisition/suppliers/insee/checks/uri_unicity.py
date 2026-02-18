from __future__ import annotations


from pathlib import Path
import logging
import pystache
from duckdb import DuckDBPyConnection
from typing import TYPE_CHECKING

from .abstract import DataValidationAndConsistencyInseeCog
from .error_handler import ErrorHandlerRemoveDuplicatedURIAfterDownloadInseeCog

if TYPE_CHECKING:
    from ..requests import RequestCOG
    from ..config import URIsUnicityInseeExceptionsToIgnoreOrCorrect


class CheckURIUnicityAfterDownloadInseeCog(DataValidationAndConsistencyInseeCog):
    def __init__(
            self,
            uri_unicity_exceptions: URIsUnicityInseeExceptionsToIgnoreOrCorrect
        ):
        super().__init__()
        self.uri_unicity_exceptions = uri_unicity_exceptions

    def handle_exceptions(self, request: RequestCOG, duckdb_conn: DuckDBPyConnection):
        """Handle exceptions in URI format checks"""
        template_path = Path(__file__).parent.parent / "sql" / "duplicated_uri_correct.mustashe.sql"
        renderer = pystache.Renderer(escape=lambda s: s) 
        
        uri_removals = [exception for exception in self.uri_unicity_exceptions.root if isinstance(exception, ErrorHandlerRemoveDuplicatedURIAfterDownloadInseeCog)]

        is_duplicated_sql = "false"
                    
        if uri_removals:
            is_duplicated_sql = "uri in (" + ", ".join([f"'{exception.uri}'" for exception in uri_removals]) + ")"

        output_path: Path = request.output_path_after_checks if isinstance(request.output_path_after_checks, Path) else Path(request.output_path_after_checks)
        output_path_tmp = Path(output_path).with_suffix(".tmp")
    
        
        context: dict[str, str] = {
            "is_duplicated_sql": is_duplicated_sql,
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
        """Check if the content of the file is valid in term of unicity of the URI"""
        template_path = Path(__file__).parent.parent / "sql" / "duplicated_uri_check.mustashe.sql"
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
                uri_bug = data_bug[0][0]
                duplicate_rows_bug = data_bug[0][1]
                if allow_exceptions and uri_bug in [exception.uri for exception in self.uri_unicity_exceptions.root]:
                    self.handle_exceptions(request=request, duckdb_conn=duckdb_conn)
                    return False
                else:
                    raise RuntimeError(f"Failed to load {request.description} after downloading. The file may be corrupted or not in the expected format. The URI {uri_bug} is duplicated at rows {duplicate_rows_bug}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error while checking duplicated URI of {request.description} after downloading") from e
        
        logging.info(f"Successfully checked duplicated URI of {request.description} after downloading")
        return True

