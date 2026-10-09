from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING

import pystache
from duckdb import DuckDBPyConnection

from .abstract import DataValidationAndConsistencyLaPosteHexasmal

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from ..requests import RequestLaPosteHexasmal


class CheckPatternAfterDownloadLaPosteHexasmal(
    DataValidationAndConsistencyLaPosteHexasmal
):
    def __init__(self, colname: str, pattern: str):
        super().__init__()
        self.colname = colname
        self.pattern = pattern

    def run(
        self, request: RequestLaPosteHexasmal, duckdb_conn: DuckDBPyConnection
    ) -> bool:
        """Check if the content of the file is valid according to a pattern"""
        template_path = (
            Path(__file__).parent.parent / "sql" / "pattern_check.mustashe.sql"
        )
        renderer = pystache.Renderer(escape=lambda s: s)
        context: dict[str, str] = {
            "view_name": request.view_name,
            "colname": self.colname,
            "pattern": self.pattern,
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
                colname_bug = data_bug[0][1]
                raise RuntimeError(
                    re.sub(
                        string=f"""
                        Failed to load La Poste Hexasmal data after downloading.
                        The file may be corrupted or not in the expected format.
                        Value '{colname_bug}' for colname {self.colname}
                        is not valid at row {row_number_bug}
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
                    Unexpected error while checking '{self.colname}
                    of La Poste Hexasmal data after downloading
                    """.repace("\r\n", "\n")
                    .replace("\n", " ")
                    .replace("\t", " "),
                    pattern=r"[ ]+",
                    repl=" ",
                )
            ) from e

        logger.info(
            re.sub(
                string=f"""
                Successfully checked pattern for colname '{self.colname}'
                of La Poste Hexasmal data after downloading
                """.repace("\r\n", "\n")
                .replace("\n", " ")
                .replace("\t", " "),
                pattern=r"[ ]+",
                repl=" ",
            )
        )
        return True
