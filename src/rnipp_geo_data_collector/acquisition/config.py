import json
from pathlib import Path

import yaml
from pydantic import BaseModel, ValidationError
from pydantic_settings import SettingsConfigDict

from .suppliers.insee.config import (
    InseeExceptionsToIgnoreOrCorrect,
    InseeSupplierConfig,
)
from .suppliers.laposte.config import (
    LaPosteExceptionsToIgnoreOrCorrect,
    LaPosteSupplierConfig,
)
from .suppliers.wikidata import WikidataSupplierConfig


def _load_config_data(file_path: Path) -> dict:
    """Load the configuration data from a JSON or YAML file."""
    if file_path.suffix == ".json":
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                config_data = json.load(file)
        except (OSError, json.JSONDecodeError) as e:
            raise ValueError(f"Error loading JSON config file: {e}") from e
    elif file_path.suffix == ".yaml":
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                config_data = yaml.safe_load(file)
        except (OSError, yaml.YAMLError) as e:
            raise ValueError(f"Error loading YAML config file: {e}") from e
    else:
        raise ValueError(f"Unsupported config file format: {file_path.suffix}")
    return config_data


def _from_file(cls: type[BaseModel], file_path: str | Path) -> BaseModel:
    """Build a config model from a JSON or YAML file."""
    if isinstance(file_path, str):
        file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"Config file not found: {file_path}")

    config_data = _load_config_data(file_path)
    try:
        return cls(**config_data)
    except (ValidationError, TypeError) as e:
        raise ValueError(f"Error parsing config file: {file_path}") from e


class AcquisitionConfig(BaseModel):
    insee: InseeSupplierConfig = InseeSupplierConfig()
    laposte: LaPosteSupplierConfig = LaPosteSupplierConfig()
    wikidata: WikidataSupplierConfig = WikidataSupplierConfig()

    model_config = SettingsConfigDict(
        env_prefix="GEOCOLLECT_", env_nested_delimiter="__"
    )

    @classmethod
    def from_file(cls, file_path: str | Path) -> "AcquisitionConfig":
        """Load the configuration from a YAML/JSON file."""
        return _from_file(cls, file_path)


class ErrorHandlerConfig(BaseModel):
    insee: InseeExceptionsToIgnoreOrCorrect = InseeExceptionsToIgnoreOrCorrect()
    laposte: LaPosteExceptionsToIgnoreOrCorrect = LaPosteExceptionsToIgnoreOrCorrect()

    @classmethod
    def from_file(cls, file_path: str | Path) -> "ErrorHandlerConfig":
        """Load the configuration from a YAML/JSON file."""
        return _from_file(cls, file_path)
