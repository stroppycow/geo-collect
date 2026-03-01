from __future__ import annotations

from pathlib import Path
import logging
import pystache
from duckdb import DuckDBPyConnection
from typing import TYPE_CHECKING

from .abstract import DataValidationAndConsistencyInseeCog, GlobalDataConsistencyInseeCog
if TYPE_CHECKING:
    from ..requests import RequestCOG


def check_insee_code_overlap(requests: list[RequestCOG], duckdb_conn: DuckDBPyConnection) -> bool:
    template_path = Path(__file__).parent.parent / "sql" / "insee_code_overlap_check.mustashe.sql"
    renderer = pystache.Renderer(escape=lambda s: s) 
    context: dict[str, str] = {}
    if len(requests) == 0:
        raise RuntimeError("No requests provided")
    elif len(requests) == 1:
        context["view_name"] = requests[0].view_name
    else:
        context["view_name"] = "(" + " UNION ALL ".join([f"SELECT uri, insee_code, start_date, end_date FROM {request.view_name}" for request in requests]) + ")"

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
            uri_a_bug = data_bug[0][1]
            uri_b_bug = data_bug[0][2]
            if len(requests) == 0:
                raise RuntimeError("No requests provided") 
            elif len(requests) == 1:
                request = requests[0]
                raise RuntimeError(f"Failed to load {request.description} after downloading. The file may be corrupted or not in the expected format. The INSEE code {insee_code_bug} is duplicated for the URI {uri_a_bug} and {uri_b_bug} with a non-empty intersection of dates")
            else:
                raise RuntimeError(f"Bug found in Insee code {insee_code_bug} with URIs {uri_a_bug} and {uri_b_bug} : periods of validity overlap detected.")
    except Exception as e:
        raise RuntimeError(f"Unexpected error while checking INSEE code overlap") from e

    if len(requests) == 0:
        raise RuntimeError("No requests provided")
    elif len(requests) == 1:
        request = requests[0]
        logging.info(f"Successfully checked INSEE code overlap of {request.description} after downloading")
    else:
        logging.info("Successfully checked INSEE code overlap of all requests")
    return True

class CheckInseeCodeOverlapAfterDownloadInseeCog(DataValidationAndConsistencyInseeCog):
    def __init__(
            self
        ):
        super().__init__()

    def run(self, request: RequestCOG, duckdb_conn: DuckDBPyConnection) -> bool:
        """Check if the content of the file is valid for the Insee code overlap"""
        return check_insee_code_overlap(requests=[request], duckdb_conn=duckdb_conn)

class CheckGlobalInseeCodeOverlapAfterDownloadInseeCog(GlobalDataConsistencyInseeCog):
    def __init__(
            self
        ):
        super().__init__()

    def run(self, requests: list[RequestCOG], duckdb_conn: DuckDBPyConnection) -> bool:
        """Check if the content of the file is valid for the Insee code overlap"""
        return check_insee_code_overlap(requests=requests, duckdb_conn=duckdb_conn)
