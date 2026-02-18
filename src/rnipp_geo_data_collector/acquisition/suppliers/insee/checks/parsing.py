from __future__ import annotations

from pathlib import Path
import logging
import pystache
from duckdb import DuckDBPyConnection
from typing import TYPE_CHECKING

from .abstract import DataValidationAndConsistencyInseeCog

if TYPE_CHECKING:
    from ..requests import RequestCOG


class CheckParsingAfterDownloadInseeCog(DataValidationAndConsistencyInseeCog):
    def __init__(self):
        super().__init__()

    def handle_exceptions(self, request: RequestCOG, duckdb_conn: DuckDBPyConnection):
        """Handle exceptions raised during the check"""
        pass
        
    def run(self, request: RequestCOG, duckdb_conn: DuckDBPyConnection,  allow_exceptions: bool = False) -> bool:
        """Check if the content of the file is valid after downloading"""
        output_path: Path = request.output_path_after_checks if isinstance(request.output_path_after_checks, Path) else Path(request.output_path_after_checks)
        if output_path.exists():
            output_path.unlink()

        if output_path.parent and not output_path.parent.exists():
            output_path.parent.mkdir(parents=True, exist_ok=True)
        
        renderer_copy = pystache.Renderer(escape=lambda s: s)
        context_copy: dict[str, str] = {
            "input_path": str(request.output_path_before_checks.resolve()),
            "output_path": str(request.output_path_after_checks.resolve())
        }
        try:
            with open(request.template_copy_path, 'r', encoding='utf-8') as template_file_copy:
                template_content_copy = template_file_copy.read()
        except Exception as e:
            raise RuntimeError(f"Failed to load template file {request.template_copy_path}") from e
        
        try:
            rendered_str_copy = renderer_copy.render(template_content_copy, context_copy)
        except Exception as e:
            raise RuntimeError(f"Failed to render template file {request.template_copy_path}") from e

        try:
            duckdb_conn.execute(rendered_str_copy)
        except Exception as e:
            raise RuntimeError(f"Failed to copy {request.description} after downloading. The file may be corrupted or not in the expected format.") from e
        
        renderer_import = pystache.Renderer(escape=lambda s: s)
        context_import: dict[str, str] = {
            "view_name": request.view_name,
            "path": str(request.output_path_after_checks.resolve())
        }
        try:
            with open(request.template_import_path, 'r', encoding='utf-8') as template_file_import:
                template_content_import = template_file_import.read()
        except Exception as e:
            raise RuntimeError(f"Failed to load template file {request.template_import_path}") from e
        
        try:
            rendered_str_import = renderer_import.render(template_content_import, context_import)
        except Exception as e:
            raise RuntimeError(f"Failed to render template file {request.template_import_path}") from e

        try:
            duckdb_conn.execute(rendered_str_import)
        except Exception as e:
            raise RuntimeError(f"Failed to load {request.description} after downloading. The file may be corrupted or not in the expected format.") from e
        
        logging.info(f"Successfully loaded {request.description} after downloading")
        return True
