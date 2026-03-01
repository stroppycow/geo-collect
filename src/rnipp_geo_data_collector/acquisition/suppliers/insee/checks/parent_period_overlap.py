from __future__ import annotations

from pathlib import Path
import logging
import pystache
from duckdb import DuckDBPyConnection
from typing import TYPE_CHECKING

from .abstract import DataValidationAndConsistencyInseeCog

if TYPE_CHECKING:
    from ..requests import RequestCOG

class CheckParentPeriodOverlapAfterDownloadInseeCog(DataValidationAndConsistencyInseeCog):
    def __init__(
            self,
            parents_view_name: list[str]
        ):
        super().__init__()
        if len(parents_view_name) == 0:
            raise RuntimeError("No parents view name provided")

        self.parents_view_name = parents_view_name
        
    def run(self, request: RequestCOG, duckdb_conn: DuckDBPyConnection) -> bool:
        """Check if parent periods don't overlap"""
        template_path = Path(__file__).parent.parent / "sql" / "parent_period_overlap_check.mustashe.sql"
        renderer = pystache.Renderer(escape=lambda s: s)

        sql_import_parent = " UNION ALL ".join([f"SELECT uri as parent_uri, start_date, end_date FROM {view_name}" for view_name in self.parents_view_name])

        context: dict[str, str] = {
            "view_name_child": request.view_name,
            "sql_import_parent": sql_import_parent
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
                parent_uri_a_bug = data_bug[2]
                parent_uri_b_bug = data_bug[0][3]
                raise RuntimeError(f"Bug found in row number {row_number_bug}, uri: {uri_bug} for {request.description} : period of validity of {parent_uri_a_bug} overlaps with {parent_uri_b_bug}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error while checking absence of overlap in periods of validity for {request.description}") from e
        logging.info(f"Successfully checked absence of overlap in periods of validity for {request.description}")
        return True

