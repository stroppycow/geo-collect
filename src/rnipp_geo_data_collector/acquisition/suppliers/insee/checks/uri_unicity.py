from __future__ import annotations


from pathlib import Path
import logging
import pystache
from duckdb import DuckDBPyConnection
from typing import TYPE_CHECKING

from .abstract import DataValidationAndConsistencyInseeCog

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ..requests import RequestCOG

class CheckURIUnicityAfterDownloadInseeCog(DataValidationAndConsistencyInseeCog):
    def __init__(
            self,
        ):
        super().__init__()

    def run(self, request: RequestCOG, duckdb_conn: DuckDBPyConnection) -> bool:
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
                raise RuntimeError(f"Failed to load {request.description} after downloading. The file may be corrupted or not in the expected format. The URI {uri_bug} is duplicated at rows {duplicate_rows_bug}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error while checking duplicated URI of {request.description} after downloading") from e
        
        logger.info(f"Successfully checked duplicated URI of {request.description} after downloading")
        return True

