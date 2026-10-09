from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from duckdb import DuckDBPyConnection

if TYPE_CHECKING:
    from ..requests import RequestLaPosteHexasmal


class DataValidationAndConsistencyLaPosteHexasmal(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def run(
        self, request: RequestLaPosteHexasmal, duckdb_conn: DuckDBPyConnection
    ) -> bool:
        pass
