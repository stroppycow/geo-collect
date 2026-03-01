from __future__ import annotations

from pathlib import Path
import logging
import pystache
from duckdb import DuckDBPyConnection
from typing import TYPE_CHECKING

from .abstract import GlobalDataConsistencyInseeCog

if TYPE_CHECKING:
    from ..requests import RequestCOG

class CheckEventsConsistencyAfterDownloadInseeCog(GlobalDataConsistencyInseeCog):
    def __init__(
            self
        ):
        super().__init__()
        
    def run(self, requests: list[RequestCOG], duckdb_conn: DuckDBPyConnection) -> bool:
        """Check that the contents of the COG files are valid by ensuring that a geographic event is always linked to a single, unique date."""
        template_path = Path(__file__).parent.parent / "sql" / "events_consistency_check.mustashe.sql"
        renderer = pystache.Renderer(escape=lambda s: s) 
        if len(requests) == 0:
            raise RuntimeError("No requests provided")
        context: dict[str, str] = {
            "sql_events_extract": "(" + " UNION ALL ".join([f"SELECT start_event_uri as event_uri, strftime(start_date, '%Y-%m-%d') as event_date FROM {request.view_name} UNION ALL SELECT end_event_uri as event_uri, strftime(end_date, '%Y-%m-%d')  as event_date FROM {request.view_name} WHERE coalesce(end_event_uri, '') <> ''" for request in requests]) + ")"
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
                event_uri = data_bug[0][0]
                events_dates = data_bug[0][1]
                raise RuntimeError(f"The contents of the COG files are not consistent. The event_uri {event_uri} has multiple dates: {events_dates}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error while checking the contents of the COG files are valid by ensuring that a geographic event is always linked to a single, unique date.")
        logging.info(f"Successfully checked that each event of the COG file is linked to a single, unique date.")  
        return True
