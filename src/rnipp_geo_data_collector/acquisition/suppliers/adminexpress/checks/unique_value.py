from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING

import pystache
from duckdb import DuckDBPyConnection

from .abstract import DataValidationAndConsistencyAdminExpressCog

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from ..requests import RequestAdminExpressCog


class CheckUniqueValueAfterDownloadAdminExpressCog(
    DataValidationAndConsistencyAdminExpressCog
):
    def __init__(self, colname: str):
        super().__init__()
        self.colname = colname

    def run(
        self, request: RequestAdminExpressCog, duckdb_conn: DuckDBPyConnection
    ) -> bool:
        """Check that a column has no duplicated value"""
        template_path = (
            Path(__file__).parent.parent / "sql" / "unique_value_check.mustashe.sql"
        )
        renderer = pystache.Renderer(escape=lambda s: s)
        context: dict[str, str] = {
            "view_name": request.view_name,
            "colname": self.colname,
        }

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
                value_bug = data_bug[0][0]
                count_bug = data_bug[0][1]
                raise RuntimeError(
                    re.sub(
                        string=f"""
                        Failed to load {request.description} data after downloading.
                        The column '{self.colname}' must have unique values,
                        but the value '{value_bug}' appears {count_bug} times
                        """.replace("\r\n", "\n")
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
                    Unexpected error while checking uniqueness of '{self.colname}'
                    of {request.description} data after downloading
                    """.replace("\r\n", "\n")
                    .replace("\n", " ")
                    .replace("\t", " "),
                    pattern=r"[ ]+",
                    repl=" ",
                )
            ) from e

        logger.info(
            re.sub(
                string=f"""
                Successfully checked uniqueness of colname '{self.colname}'
                of {request.description} data after downloading
                """.replace("\r\n", "\n")
                .replace("\n", " ")
                .replace("\t", " "),
                pattern=r"[ ]+",
                repl=" ",
            )
        )
        return True
