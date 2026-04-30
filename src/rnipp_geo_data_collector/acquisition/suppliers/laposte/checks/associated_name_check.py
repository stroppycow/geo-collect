from __future__ import annotations

from pathlib import Path
import logging
import pystache
from duckdb import DuckDBPyConnection
from typing import TYPE_CHECKING

from .abstract import DataValidationAndConsistencyLaPosteHexasmal


if TYPE_CHECKING:
    from ..requests import RequestLaPosteHexasmal

class CheckAssociatedNameAfterDownloadLaPosteHexasmal(DataValidationAndConsistencyLaPosteHexasmal):
    def __init__(
            self
        ):
        super().__init__()


    
    def run(self, request: RequestLaPosteHexasmal, duckdb_conn: DuckDBPyConnection) -> bool:
        """Check that if an INSEE code is associated with multiple rows/observations, there is at most one missing value in the additional labels (e.g., row_5)"""
        template_path = Path(__file__).parent.parent / "sql" / "associated_name_check.mustashe.sql"
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
                insee_code_bug = data_bug[0][0]
                postal_code_bug = data_bug[0][1]
                associated_names_bug = data_bug[0][2]
                raise RuntimeError(f"Failed to load La Poste Hexasmal data after downloading. The file may be corrupted or not in the expected format. (INSEE code {insee_code_bug}, Postal Code {postal_code_bug}) is associated with multiple rows/observations, but the additional labels (i.e., ligne_5) have more than one missing value: {associated_names_bug}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error while checking multiple associated names of La Poste Hexasmal data after downloading") from e
                  
        logging.info(f"Successfully checked that if an INSEE code is associated with multiple rows/observations, the additional labels (i.e., ligne_5) have at most one missing value of La Poste Hexasmal data after downloading")
        return True
