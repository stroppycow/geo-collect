from __future__ import annotations

from pathlib import Path
import logging
import pystache
from duckdb import DuckDBPyConnection
from typing import TYPE_CHECKING
import datetime

from .abstract import DataValidationAndConsistencyInseeCog

if TYPE_CHECKING:
    from ..requests import RequestCOG


class CheckStartDateAfterDownloadInseeCog(DataValidationAndConsistencyInseeCog):
    def __init__(
            self,
        ):
        super().__init__()

    def run(self, request: RequestCOG, duckdb_conn: DuckDBPyConnection) -> bool:
        """Check if the content of the file is valid for the start date variable"""
        template_path = Path(__file__).parent.parent / "sql" /  "start_date_check.mustashe.sql"
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
                start_date_bug = data_bug[0][2]
                start_date_count_bug = data_bug[0][3]
                date_category_bug = data_bug[0][4]
                if date_category_bug == 'NULL':
                    raise RuntimeError(f"Failed to load {request.description} after downloading. The file may be corrupted or not in the expected format. The start date is empty at row {row_number_bug} for the URI {uri_bug}")
                elif date_category_bug == 'BEFORE':
                    raise RuntimeError(f"Failed to load {request.description} after downloading. The file may be corrupted or not in the expected format. The start date {start_date_bug} is older than the minimum date 1943-01-01 at row {row_number_bug} for the URI {uri_bug}")
                elif date_category_bug == 'AFTER':
                    raise RuntimeError(f"Failed to load {request.description} after downloading. The file may be corrupted or not in the expected format. The start date {start_date_bug} is newer than the maximum date {datetime.datetime.now().strftime('%Y-%m-%d')} at row {row_number_bug} for the URI {uri_bug}")
                elif date_category_bug == 'MULTIPLE':
                    raise RuntimeError(f"Failed to load {request.description} after downloading. The file may be corrupted or not in the expected format. The start date is duplicated {start_date_count_bug} for the URI {uri_bug}")
                else:
                    raise RuntimeError(f"Unexpected error while checking start date of {request.description} after downloading")
        except Exception as e:
            raise RuntimeError(f"Unexpected error while checking start date of {request.description} after downloading") from e

        logging.info(f"Successfully checked start date of {request.description} after downloading")
        return True
