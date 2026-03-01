from __future__ import annotations
from duckdb import DuckDBPyConnection
from typing import TYPE_CHECKING, Optional
from abc import ABC, abstractmethod


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
