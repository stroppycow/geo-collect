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


class CheckGeometryValidAfterDownloadAdminExpressCog(
    DataValidationAndConsistencyAdminExpressCog
):
    def __init__(
        self, geometry_colname: str = "geometrie", key_colname: str = "cleabs"
    ):
        super().__init__()
        self.geometry_colname = geometry_colname
        self.key_colname = key_colname

    def run(
        self, request: RequestAdminExpressCog, duckdb_conn: DuckDBPyConnection
    ) -> bool:
        """Check that every geometry is non-null, non-empty and valid (OGC)"""
        template_path = (
            Path(__file__).parent.parent / "sql" / "geometry_valid_check.mustashe.sql"
        )
        renderer = pystache.Renderer(escape=lambda s: s)
        context: dict[str, str] = {
            "view_name": request.view_name,
            "geometry_colname": self.geometry_colname,
            "key_colname": self.key_colname,
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
                key_bug = data_bug[0][1]
                raise RuntimeError(
                    re.sub(
                        string=f"""
                        Failed to load {request.description} data after downloading.
                        The geometry of row {row_number_bug} ('{key_bug}')
                        is NULL, empty or not OGC-valid
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
                    Unexpected error while checking geometry validity of
                    {request.description} data after downloading
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
                Successfully checked geometry validity of
                {request.description} data after downloading
                """.replace("\r\n", "\n")
                .replace("\n", " ")
                .replace("\t", " "),
                pattern=r"[ ]+",
                repl=" ",
            )
        )
        return True
