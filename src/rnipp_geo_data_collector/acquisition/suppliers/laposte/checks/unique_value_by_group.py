from __future__ import annotations

from pathlib import Path
import logging
import pystache
from duckdb import DuckDBPyConnection
from typing import TYPE_CHECKING

from .abstract import DataValidationAndConsistencyLaPosteHexasmal


if TYPE_CHECKING:
    from ..requests import RequestLaPosteHexasmal

class CheckUniqueValueByGroupAfterDownloadLaPosteHexasmal(DataValidationAndConsistencyLaPosteHexasmal):
    def __init__(
            self,
            colname: str,
            group_colname: str
        ):
        super().__init__()
        self.colname = colname
        self.group_colname = group_colname

    
    def run(self, request: RequestLaPosteHexasmal, duckdb_conn: DuckDBPyConnection) -> bool:
        """Check if the content of the file is valid whith a unique value for a column by group"""
        template_path = Path(__file__).parent.parent / "sql" / "unique_value_by_group_check.mustashe.sql"
        renderer = pystache.Renderer(escape=lambda s: s) 
        context: dict[str, str] = {
            "view_name": request.view_name,
            "colname": self.colname,
            "group_colname": self.group_colname
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
                group_colname_bug = data_bug[0][0]
                colname_bug = data_bug[0][1]
                raise RuntimeError(f"Failed to load La Poste Hexasmal data after downloading. The file may be corrupted or not in the expected format. Value '{colname_bug}' for colname {self.colname} is not unique for group {group_colname_bug}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error while checking unique value for colname '{self.colname}' by group '{self.group_colname}' of La Poste Hexasmal data after downloading") from e
                  
        logging.info(f"Successfully checked unique value for colname '{self.colname}' by group '{self.group_colname}' of La Poste Hexasmal data after downloading")
        return True
