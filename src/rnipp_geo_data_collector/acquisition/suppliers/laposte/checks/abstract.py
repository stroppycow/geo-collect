from __future__ import annotations
from duckdb import DuckDBPyConnection
from typing import TYPE_CHECKING, Optional
from abc import ABC, abstractmethod


if TYPE_CHECKING:
    from ..requests import RequestLaPosteHexasmal


class DataValidationAndConsistencyLaPosteHexasmal(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def run(self, request: RequestLaPosteHexasmal, duckdb_conn: DuckDBPyConnection) -> bool:
        pass
