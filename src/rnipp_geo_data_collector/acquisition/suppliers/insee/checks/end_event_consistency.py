from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING

import pystache
from duckdb import DuckDBPyConnection

from .abstract import DataValidationAndConsistencyInseeCog

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ..requests import RequestCOG


class CheckEndEventConsistencyAfterDownloadInseeCog(
    DataValidationAndConsistencyInseeCog
):
    def __init__(self):
        super().__init__()

    def run(self, request: RequestCOG, duckdb_conn: DuckDBPyConnection) -> bool:
        """Check if the content of the file is valid for the end event consistency"""
        template_path = (
            Path(__file__).parent.parent
            / "sql"
            / "end_event_consistency_check.mustashe.sql"
        )
        renderer = pystache.Renderer(escape=lambda s: s)
        context: dict[str, str] = {"view_name": request.view_name}

        try:
            with open(template_path, "r", encoding="utf-8") as template_file:
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
                end_event_bug = data_bug[0][2]
                end_date_bug = data_bug[0][3]

                if (end_event_bug is None) or (end_event_bug == ""):
                    raise RuntimeError(
                        re.sub(
                            string=f"""
                            Failed to load {request.description} after downloading.
                            The file may be corrupted or not in the expected format.
                            The end event is empty whereas end date is not empty.
                            Row number: {row_number_bug}, URI: {uri_bug},
                            end_event: {end_event_bug}, end_date: {end_date_bug}.
                            """.repace("\r\n", "\n")
                            .replace("\n", " ")
                            .replace("\t", " "),
                            pattern=r"[ ]+",
                            repl=" ",
                        )
                    )
                else:
                    raise RuntimeError(
                        re.sub(
                            string=f"""
                            Failed to load {request.description} after downloading.
                            The file may be corrupted or not in the expected format.
                            The end event is not empty whereas end date is empty.
                            Row number: {row_number_bug}, URI: {uri_bug},
                            end_event: {end_event_bug}, end_date: {end_date_bug}.
                            """.repace("\r\n", "\n")
                            .replace("\n", " ")
                            .replace("\t", " "),
                            pattern=r"[ ]+",
                            repl=" ",
                        )
                    )
        except Exception as e:
            raise RuntimeError(
                re.sub(
                    string=f"""
                    Unexpected error while checking end event consistency
                    of {request.description} after downloading
                    """.repace("\r\n", "\n")
                    .replace("\n", " ")
                    .replace("\t", " "),
                    pattern=r"[ ]+",
                    repl=" ",
                )
            ) from e
        logger.info(
            f"Successfully checked end event consistency of {request.description} after downloading"
        )
        return True
