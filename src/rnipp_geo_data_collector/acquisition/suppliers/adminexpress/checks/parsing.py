from __future__ import annotations

import re
from typing import TYPE_CHECKING

import pystache
from duckdb import DuckDBPyConnection

from .abstract import DataValidationAndConsistencyAdminExpressCog

if TYPE_CHECKING:
    from ..requests import RequestAdminExpressCog


class CheckParsingAfterDownloadAdminExpressCog(
    DataValidationAndConsistencyAdminExpressCog
):
    def __init__(self):
        super().__init__()

    def copy(self, request: RequestAdminExpressCog, duckdb_conn: DuckDBPyConnection):
        if not request.output_paths.cleaned_entities.parent.exists():
            request.output_paths.cleaned_entities.parent.mkdir(
                parents=True, exist_ok=True
            )

        if request.output_paths.cleaned_entities.exists():
            request.output_paths.cleaned_entities.unlink()

        renderer_copy = pystache.Renderer(escape=lambda s: s)
        context_copy: dict[str, str] = {
            "input_path": str(request.output_paths.raw_entities.resolve()),
            "output_path": str(request.output_paths.cleaned_entities.resolve()),
        }
        try:
            with open(
                request.sql_templates.copy, "r", encoding="utf-8"
            ) as template_file_copy:
                template_content_copy = template_file_copy.read()
        except Exception as e:
            raise RuntimeError(
                f"Failed to load template file {request.sql_templates.copy}"
            ) from e

        try:
            rendered_str_copy = renderer_copy.render(
                template_content_copy, context_copy
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to render template file {request.sql_templates.copy}"
            ) from e

        try:
            duckdb_conn.execute(rendered_str_copy)
        except Exception as e:
            raise RuntimeError(
                re.sub(
                    string="""
                    Failed to copy Admin Express COG data after downloading.
                    The file may be corrupted or not in the expected GeoParquet format.
                    """.replace("\r\n", "\n")
                    .replace("\n", " ")
                    .replace("\t", " "),
                    pattern=r"[ ]+",
                    repl=" ",
                )
            ) from e

    def create_view(
        self, request: RequestAdminExpressCog, duckdb_conn: DuckDBPyConnection
    ):
        renderer_import = pystache.Renderer(escape=lambda s: s)
        context_import: dict[str, str] = {
            "view_name": request.view_name,
            "path": str(request.output_paths.cleaned_entities.resolve()),
        }
        try:
            with open(
                request.sql_templates.create_view, "r", encoding="utf-8"
            ) as template_file_import:
                template_content_import = template_file_import.read()
        except Exception as e:
            raise RuntimeError(
                f"Failed to load template file {request.sql_templates.create_view}"
            ) from e

        try:
            rendered_str_import = renderer_import.render(
                template_content_import, context_import
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to render template file {request.sql_templates.create_view}"
            ) from e

        try:
            duckdb_conn.execute(rendered_str_import)
        except Exception as e:
            raise RuntimeError(
                re.sub(
                    string=f"""
                    Failed to load {request.description} data after downloading.
                    The file may be corrupted or not in the expected GeoParquet format.
                    """.replace("\r\n", "\n")
                    .replace("\n", " ")
                    .replace("\t", " "),
                    pattern=r"[ ]+",
                    repl=" ",
                )
            ) from e

    def run(
        self, request: RequestAdminExpressCog, duckdb_conn: DuckDBPyConnection
    ) -> bool:
        """Check if the content of the file is valid after downloading"""
        self.copy(request=request, duckdb_conn=duckdb_conn)
        self.create_view(request=request, duckdb_conn=duckdb_conn)
        request.apply_updates(duckdb_conn=duckdb_conn)
        self.create_view(request=request, duckdb_conn=duckdb_conn)
        return True
