from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from duckdb import DuckDBPyConnection

if TYPE_CHECKING:
    from ..requests import RequestCOG


class DataValidationAndConsistencyInseeCog(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def run(self, request: RequestCOG, duckdb_conn: DuckDBPyConnection) -> bool:
        pass


class GlobalDataConsistencyInseeCog(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def run(self, requests: list[RequestCOG], duckdb_conn: DuckDBPyConnection) -> bool:
        pass
