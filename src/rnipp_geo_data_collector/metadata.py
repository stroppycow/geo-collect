
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Optional

class ColumnDataType(ABC):
    def __init__(
            self,
            name: str,
            duckdb_type: str
        ):
        self.name = name
        self.duckdb_type = duckdb_type

    @abstractmethod
    def get_duckdb_parsing(self) -> str:
        pass

    def __eq__(self, other):
        if not isinstance(other, ColumnDataType):
            return False
        return self.name == other.name and self.duckdb_type == other.duckdb_type

class StringColumnDataType(ColumnDataType):
    def __init__(self, name: str):
        super().__init__(
            name=name,
            duckdb_type="VARCHAR"
        )

    def get_duckdb_parsing(self) -> str:
        return "{x}".format(x=self.name)

    def __eq__(self, other):
        if not isinstance(other, StringColumnDataType):
            return False
        return super().__eq__(other)

class StringListColumnDataType(ColumnDataType):
    def __init__(self, name: str, sep: str):
        super().__init__(
            name=name,
            duckdb_type="VARCHAR"
        )
        self.sep = sep

    def get_duckdb_parsing(self) -> str:
        return "{x}".format(x=self.name)

    def __eq__(self, other):
        if not isinstance(other, StringListColumnDataType):
            return False
        return super().__eq__(other) and self.sep == other.sep

class BooleanColumnDataType(ColumnDataType):
    def __init__(self, name: str):
        super().__init__(
            name=name,
            duckdb_type="BOOLEAN"
        )

    def get_duckdb_parsing(self) -> str:
        return "CAST({x} AS BOOLEAN)".format(x=self.name)

    def __eq__(self, other):
        if not isinstance(other, BooleanColumnDataType):
            return False
        return super().__eq__(other)

class DateColumnDataType(ColumnDataType):
    def __init__(self, name: str, format: str):
        super().__init__(
            name=name,
            duckdb_type="DATE"
        )
        self.format = format

    def get_duckdb_parsing(self) -> str:
        return "CASE WHEN {x} is NULL THEN NULL::DATE ELSE CAST(strptime({x}, '{format}') AS DATE) END".format(x=self.name, format=self.format)

    def __eq__(self, other):
        if not isinstance(other, DateColumnDataType):
            return False
        return super().__eq__(other) and self.format == other.format

class IntegerColumnDataType(ColumnDataType):
    def __init__(self, name: str):
        super().__init__(
            name=name,
            duckdb_type="BIGINT"
        )

    def get_duckdb_parsing(self) -> str:
        return "CASE WHEN {x} is NULL THEN NULL::BIGINT ELSE CAST({x} AS BIGINT) END".format(x=self.name)

    def __eq__(self, other):
        if not isinstance(other, IntegerColumnDataType):
            return False
        return super().__eq__(other)


class FileMetadata(ABC):
    def __init__(self, path: Path):
        self.path = path

class GeoCSVFileMetadata(FileMetadata):
    def __init__(
            self,
            path: Path,
            header: bool,
            delim: str,
            encoding: str,
            colnames: list[ColumnDataType]
        ):
        super().__init__(path=path)
        self.header = header
        self.delim =  delim
        self.encoding = encoding
        self.colnames = colnames

    def get_duckdb_sql_import_query(self, keep_colnames: Optional[list[str]] = None) -> str:
        return """
            SELECT 
                {columns_spec}
            FROM read_csv(
                '{path}',
                delim = '{delim}',
                header = {header},
                all_varchar = true,
                encoding='{encoding}'
            )
        """.format(
            columns_spec=", ".join([f"{c.get_duckdb_parsing()} AS {c.name}" for c in self.colnames if keep_colnames is None or c.name in keep_colnames]),
            path=str(self.path.resolve()),
            delim=self.delim,
            header='true' if self.header else 'false',
            encoding=self.encoding
        )

class InfoCurrentStoredData:
    def __init__(self):
        self.data: dict[str, FileMetadata] = {}

    def add_data(self, key: str, value: FileMetadata):
        if key in self.data:
            raise RuntimeError("Key '{key}' already exists".format(key=key))
        self.data[key] = value

    def replace_data(self, key: str, value: FileMetadata):
        if key not in self.data:
            raise RuntimeError("Key '{key}' does not exists".format(key=key))
        self.data[key] = value

    def remove_data(self, key: str):
        if key not in self.data:
            raise RuntimeError("Key '{key}' does not exists".format(key=key))
        del self.data[key]
    
    def get_data(self, key: str) -> FileMetadata:
        if key not in self.data:
            raise RuntimeError("Key '{key}' does not exists".format(key=key))
        return self.data[key]
    
    