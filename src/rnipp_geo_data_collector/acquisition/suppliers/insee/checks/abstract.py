from __future__ import annotations
from duckdb import DuckDBPyConnection
from typing import TYPE_CHECKING
from abc import ABC, abstractmethod


if TYPE_CHECKING:
    from ..requests import RequestCOG


class DataValidationAndConsistencyInseeCog(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def handle_exceptions(self, request: RequestCOG, duckdb_conn: DuckDBPyConnection):
        pass

    @abstractmethod
    def run(self, request: RequestCOG, duckdb_conn: DuckDBPyConnection, allow_exceptions: bool = False) -> bool:
        pass

