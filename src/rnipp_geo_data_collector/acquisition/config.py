from pydantic import BaseModel
from pydantic_settings import SettingsConfigDict
from pathlib import Path
from typing import Union
import json


from .suppliers.insee.config import InseeSupplierConfig, InseeExceptionsToIgnoreOrCorrect
from .suppliers.laposte.config import LaPosteSupplierConfig, LaPosteExceptionsToIgnoreOrCorrect
from .suppliers.wikidata import WikidataSupplierConfig

class AcquisitionConfig(BaseModel):
    insee: InseeSupplierConfig = InseeSupplierConfig()
    laposte: LaPosteSupplierConfig = LaPosteSupplierConfig()
    wikidata: WikidataSupplierConfig = WikidataSupplierConfig()

    model_config = SettingsConfigDict(
        env_prefix="GEOCOLLECT_",
        env_nested_delimiter="__"
    )
        

    @classmethod
    def from_file(cls, file_path: Union[str, Path]) -> "AcquisitionConfig":
        """Load the configuration from a YAML/JSON file."""
        if isinstance(file_path, str):
            file_path = Path(file_path) 
        
        if not file_path.exists():
            raise FileNotFoundError(f"Config file not found: {file_path}")

        if file_path.suffix == '.json':
            try:
                with open(file_path, 'r') as file:
                    config_data = json.load(file)
            except Exception as e:
                raise ValueError(f"Error loading JSON config file: {e}")
            try:
                output = AcquisitionConfig(**config_data)
            except Exception as e:
                raise ValueError(f"Error parsing config file: {e}")
            return output
        elif file_path.suffix == '.yaml':
            try:
                import yaml
            except ImportError:
                raise ImportError("PyYAML is required to load YAML config files. Install it with `pip install pyyaml`.")
            try:
                with open(file_path, 'r') as file:
                    config_data = yaml.safe_load(file)
            except Exception as e:
                raise ValueError(f"Error loading YAML config file: {e}")
            try:
                output = AcquisitionConfig(**config_data)
            except Exception as e:
                raise ValueError(f"Error parsing config file: {e}")
            return output
        else:
            raise ValueError(f"Unsupported config file format: {file_path.suffix}")


class ErrorHandlerConfig(BaseModel):
    insee: InseeExceptionsToIgnoreOrCorrect = InseeExceptionsToIgnoreOrCorrect()
    laposte: LaPosteExceptionsToIgnoreOrCorrect = LaPosteExceptionsToIgnoreOrCorrect()


    @classmethod
    def from_file(cls, file_path: Union[str, Path]) -> "ErrorHandlerConfig":
        """Load the configuration from a YAML/JSON file."""
        if isinstance(file_path, str):
            file_path = Path(file_path) 
        
        if not file_path.exists():
            raise FileNotFoundError(f"Config file not found: {file_path}")

        if file_path.suffix == '.json':
            try:
                with open(file_path, 'r') as file:
                    config_data = json.load(file)
            except Exception as e:
                raise ValueError(f"Error loading JSON config file: {e}")
            try:
                output = ErrorHandlerConfig(**config_data)
            except Exception as e:
                raise ValueError(f"Error parsing config file: {e}")
            return output
        elif file_path.suffix == '.yaml':
            try:
                import yaml
            except ImportError:
                raise ImportError("PyYAML is required to load YAML config files. Install it with `pip install pyyaml`.")
            try:
                with open(file_path, 'r') as file:
                    config_data = yaml.safe_load(file)
            except Exception as e:
                raise ValueError(f"Error loading YAML config file: {e}")
            try:
                output = ErrorHandlerConfig(**config_data)
            except Exception as e:
                raise ValueError(f"Error parsing config file: {e}")
            return output
        else:
            raise ValueError(f"Unsupported config file format: {file_path.suffix}")
