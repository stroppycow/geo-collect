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


class CheckSingleAssociatedNameAfterDownloadLaPosteHexasmal(
    DataValidationAndConsistencyLaPosteHexasmal
):
    def __init__(self):
        super().__init__()

    def run(
        self, request: RequestLaPosteHexasmal, duckdb_conn: DuckDBPyConnection
    ) -> bool:
        """Check whether, if an INSEE code is associated with only one row/observation,
        the additional label (i.e., ligne_5) is missing"""
        template_path = (
            Path(__file__).parent.parent
            / "sql"
            / "single_associated_name_check.mustashe.sql"
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
                insee_code_bug = data_bug[0][0]
                raise RuntimeError(
                    re.sub(
                        string=f"""
                        Failed to load La Poste Hexasmal data after downloading.
                        The file may be corrupted or not in the expected format.
                        INSEE code {insee_code_bug} is associated with only one
                        row/observation, but the additional label (i.e., ligne_5) is missing
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
                    string="""
                    "Unexpected error while checking single associated name
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
                string="""
                Successfully checked whether, if an INSEE code is
                associated with only one row/observation,
                the additional label (i.e., ligne_5) is missing
                of La Poste Hexasmal data after downloading
                """.repace("\r\n", "\n")
                .replace("\n", " ")
                .replace("\t", " "),
                pattern=r"[ ]+",
                repl=" ",
            )
        )
        return True
