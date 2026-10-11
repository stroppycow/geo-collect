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


class CheckNotNullAfterDownloadInseeCog(DataValidationAndConsistencyInseeCog):
    def __init__(self, colname: str):
        super().__init__()
        self.colname = colname

    def run(self, request: RequestCOG, duckdb_conn: DuckDBPyConnection) -> bool:
        """
        Check if the content of the file is valid by checking
        if the column contains only non-null values
        """
        template_path = (
            Path(__file__).parent.parent / "sql" / "not_null_check.mustashe.sql"
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
                row_number_bug = data_bug[0][0]
                uri_bug = data_bug[0][1]
                colname_bug = data_bug[0][2]
                if self.colname == "uri":
                    raise RuntimeError(
                        re.sub(
                            string=f"""
                            Failed to load {request.description} after downloading.
                            The file may be corrupted or not in the expected format.
                            The URI {uri_bug} is not valid at row {row_number_bug}
                            """.replace("\r\n", "\n")
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
                            Value '{colname_bug}' for colname {self.colname} is
                            not valid at row {row_number_bug} and URI = {uri_bug}
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
                    Unexpected error while checking colname '{self.colname}'
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
                Successfully checked not null for colname '{self.colname}'
                of {request.description} after downloading
                """.replace("\r\n", "\n")
                .replace("\n", " ")
                .replace("\t", " "),
                pattern=r"[ ]+",
                repl=" ",
            )
        )
        return True
