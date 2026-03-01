from __future__ import annotations

from pathlib import Path
import logging
import pystache
from duckdb import DuckDBPyConnection
from typing import TYPE_CHECKING

from .abstract import DataValidationAndConsistencyInseeCog

if TYPE_CHECKING:
    from ..requests import RequestCOG

class CheckEventsUnequalAfterDownloadInseeCog(DataValidationAndConsistencyInseeCog):
    def __init__(
            self
        ):
        super().__init__()
        
    def run(self, request: RequestCOG, duckdb_conn: DuckDBPyConnection) -> bool:
        """Check if the content of the file is valid : start event uri is distinct from end event uri."""
        template_path = Path(__file__).parent.parent / "sql" / "events_unequal_check.mustashe.sql"
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
                start_event_bug = data_bug[0][2]
                end_event_bug = data_bug[0][3]
                raise RuntimeError(f"Failed to load {request.description} after downloading. The file may be corrupted or not in the expected format. The start event URI is the same as the end event URI. Row number: {row_number_bug}, URI: {uri_bug}, Start Event URI: {start_event_bug}, End Event URI: {end_event_bug}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error while checking start event URI and end event URI are not equal for {request.description} after downloading") from e
        logging.info(f"Successfully checked that the start event URI and the end event URI are not equal for  {request.description} after downloading")
        return True

