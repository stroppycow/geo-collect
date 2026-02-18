from __future__ import annotations
from pathlib import Path
import logging
import pystache
from duckdb import DuckDBPyConnection
from typing import TYPE_CHECKING

from .abstract import DataValidationAndConsistencyInseeCog
from .error_handler import ErrorHandlerReplaceBadArticleCodeAfterDownloadInseeCog, ErrorHandlerRemoveBadArticleCodeAfterDownloadInseeCog


if TYPE_CHECKING:
    from ..requests import RequestCOG
    from ..config import ArticleCodeInseeExceptionsToIgnoreOrCorrect



class CheckArticleCodeAfterDownloadInseeCog(DataValidationAndConsistencyInseeCog):
    def __init__(
            self,
            article_code_exceptions: ArticleCodeInseeExceptionsToIgnoreOrCorrect,
            pattern: str = r"^[0-8X]$"
        ):
        super().__init__()
        self.article_code_exceptions = article_code_exceptions
        self.pattern = pattern

    def handle_exceptions(self, request: RequestCOG, duckdb_conn: DuckDBPyConnection):
        """Handle exceptions in article code checks"""
        template_path = Path(__file__).parent.parent / "sql" / "article_code_correct.mustashe.sql"
        renderer = pystache.Renderer(escape=lambda s: s) 
        
        modif_article_code_sql = "article_code"
        filter_condition = ""

        uri_updates = [exception for exception in self.article_code_exceptions.root if isinstance(exception, ErrorHandlerReplaceBadArticleCodeAfterDownloadInseeCog)]
        uri_removals = [exception for exception in self.article_code_exceptions.root if isinstance(exception, ErrorHandlerRemoveBadArticleCodeAfterDownloadInseeCog)]
        
        if uri_updates:
            modif_article_code_sql = "CASE " + " ".join([f"WHEN uri = '{exception.uri}' THEN '{exception.article_code}'" for exception in uri_updates]) + f" ELSE article_code END"
            
        if uri_removals:
            filter_condition = "WHERE uri NOT IN (" + ", ".join([f"'{exception.uri}'" for exception in uri_removals]) + ")"

        output_path: Path = request.output_path_after_checks if isinstance(request.output_path_after_checks, Path) else Path(request.output_path_after_checks)
        output_path_tmp = Path(output_path).with_suffix(".tmp")
    
        
        context: dict[str, str] = {
            "modif_article_code_sql": modif_article_code_sql,
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
        """Check if the content of the file is valid for the article code variable"""
        template_path = Path(__file__).parent.parent  / "sql" / "article_code_check.mustashe.sql"
        renderer = pystache.Renderer(escape=lambda s: s) 
        context: dict[str, str] = {
            "view_name": request.view_name,
            "pattern": self.pattern
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
                code_insee_bug = data_bug[0][2]
                if allow_exceptions and uri_bug in [exception.uri for exception in self.article_code_exceptions.root]:
                    self.handle_exceptions(request=request, duckdb_conn=duckdb_conn)
                    return False
                else:
                    raise RuntimeError(f"Failed to load {request.description} after downloading. The file may be corrupted or not in the expected format. The Insee code {code_insee_bug} is not valid at row {row_number_bug} for the URI {uri_bug}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error while checking Insee code of {request.description} after downloading") from e

        logging.info(f"Successfully checked article code of {request.description} after downloading")
        return True

