import datetime
from typing import Any, Optional
from pydantic import BaseModel, model_validator
import validators
import re


def check_uri(uri: str):
    if not validators.url(uri):
        raise ValueError(f"Invalid URI: {uri}")

def check_insee_code(insee_code: str):
    if re.fullmatch(pattern = r"[0-9A-Za-z]*", string=insee_code) is None:
        raise ValueError(f"Invalid Insee code: {insee_code}")
    
def check_article_code(article_code: str):
    if re.fullmatch(pattern = r"[0-9A-Za-z]*", string=article_code) is None:
        raise ValueError(f"Invalid Article code: {article_code}")
    
def check_uri_list(uris: str):
    if uris == "":
        raise ValueError("URIs list cannot be empty")
    for uri in uris.split("|"):
        check_uri(uri=uri)

class InseeGeoAddOrReplace(BaseModel):
    pass

    def to_dict_csv_export(self) -> dict[str, Any]:
        return {}


class InseeCommuneAddOrReplace(InseeGeoAddOrReplace):
    uri : str
    insee_code : str
    label: str
    article_code: str
    parent_uri: str
    start_event_uri: str
    end_event_uri: Optional[str] = None
    start_date: datetime.date
    end_date: Optional[datetime.date] = None

    @model_validator(mode="after")
    def check_uri(self):
        check_uri(uri=self.uri)
        return self
    
    @model_validator(mode="after")
    def check_insee_code(self):
        check_insee_code(insee_code=self.insee_code)
        return self
    
    @model_validator(mode="after")
    def check_article_code(self):
        check_article_code(article_code=self.article_code)
        return self
    
    @model_validator(mode="after")
    def check_parent_uri(self):
        check_uri_list(uris=self.parent_uri)
        return self
    
    @model_validator(mode="after")
    def check_date(self):
        if self.end_date is not None and self.end_date < self.start_date:
            raise ValueError(f"Invalid date: {self.end_date} < {self.start_date}")
        return self
    
    @model_validator(mode="after")
    def check_start_event_uri(self):
        check_uri(uri=self.start_event_uri)
        return self
    
    @model_validator(mode="after")
    def check_end_event_uri(self):
        if self.end_event_uri is not None:
            check_uri(uri=self.end_event_uri)
        return self
    
    @model_validator(mode="after")
    def check_end_consistency(self):
        if self.end_event_uri is not None and self.end_date is None:
            raise ValueError("end_date must be provided if end_event_uri is provided")
        if self.end_date is not None and self.end_event_uri is None:
            raise ValueError("end_event_uri must be provided if end_date is provided")
        return self
    
    def to_dict_csv_export(self) -> dict[str, Any]:
        return {
            'uri': self.uri,
            'insee_code': self.insee_code,
            'label': self.label,
            'article_code': self.article_code,
            'parent_uri': self.parent_uri,
            'start_event_uri': self.start_event_uri,
            'end_event_uri': self.end_event_uri,
            'start_date': self.start_date.isoformat(),
            'end_date': self.end_date.isoformat() if isinstance(self.end_date, datetime.date) else None,
            'parent_uri_count': len(self.parent_uri.split('|')),
            'start_date_count': 1,
            'end_date_count': 1 if isinstance(self.end_date, datetime.date) else 0 
        }

class InseeArrondissementMunicipalAddOrReplace(InseeGeoAddOrReplace):
    uri : str
    insee_code : str
    label: str
    article_code: str
    parent_uri: str
    start_event_uri: str
    end_event_uri: Optional[str] = None
    start_date: datetime.date
    end_date: Optional[datetime.date] = None

    @model_validator(mode="after")
    def check_uri(self):
        check_uri(uri=self.uri)
        return self
    
    @model_validator(mode="after")
    def check_insee_code(self):
        check_insee_code(insee_code=self.insee_code)
        return self
    
    @model_validator(mode="after")
    def check_article_code(self):
        check_article_code(article_code=self.article_code)
        return self
    
    @model_validator(mode="after")
    def check_parent_uri(self):
        check_uri_list(uris=self.parent_uri)
        return self
    
    @model_validator(mode="after")
    def check_date(self):
        if self.end_date is not None and self.end_date < self.start_date:
            raise ValueError(f"Invalid date: {self.end_date} < {self.start_date}")
        return self
    
    @model_validator(mode="after")
    def check_start_event_uri(self):
        check_uri(uri=self.start_event_uri)
        return self
    
    @model_validator(mode="after")
    def check_end_event_uri(self):
        if self.end_event_uri is not None:
            check_uri(uri=self.end_event_uri)
        return self
    
    @model_validator(mode="after")
    def check_end_consistency(self):
        if self.end_event_uri is not None and self.end_date is None:
            raise ValueError("end_date must be provided if end_event_uri is provided")
        if self.end_date is not None and self.end_event_uri is None:
            raise ValueError("end_event_uri must be provided if end_date is provided")
        return self
    
    def to_dict_csv_export(self) -> dict[str, Any]:
        return {
            'uri': self.uri,
            'insee_code': self.insee_code,
            'label': self.label,
            'article_code': self.article_code,
            'parent_uri': self.parent_uri,
            'start_event_uri': self.start_event_uri,
            'end_event_uri': self.end_event_uri,
            'start_date': self.start_date.isoformat(),
            'end_date': self.end_date.isoformat() if isinstance(self.end_date, datetime.date) else None,
            'parent_uri_count': len(self.parent_uri.split('|')),
            'start_date_count': 1,
            'end_date_count': 1 if isinstance(self.end_date, datetime.date) else 0 
        }


class InseeDepartementAddOrReplace(InseeGeoAddOrReplace):
    uri : str
    insee_code : str
    label: str
    article_code: str
    start_event_uri: str
    end_event_uri: Optional[str] = None
    start_date: datetime.date
    end_date: Optional[datetime.date] = None

    @model_validator(mode="after")
    def check_uri(self):
        check_uri(uri=self.uri)
        return self
    
    @model_validator(mode="after")
    def check_insee_code(self):
        check_insee_code(insee_code=self.insee_code)
        return self
    
    @model_validator(mode="after")
    def check_article_code(self):
        check_article_code(article_code=self.article_code)
        return self

    @model_validator(mode="after")
    def check_date(self):
        if self.end_date is not None and self.end_date < self.start_date:
            raise ValueError(f"Invalid date: {self.end_date} < {self.start_date}")
        return self
    
    @model_validator(mode="after")
    def check_start_event_uri(self):
        check_uri(uri=self.start_event_uri)
        return self
    
    @model_validator(mode="after")
    def check_end_event_uri(self):
        if self.end_event_uri is not None:
            check_uri(uri=self.end_event_uri)
        return self
    
    @model_validator(mode="after")
    def check_end_consistency(self):
        if self.end_event_uri is not None and self.end_date is None:
            raise ValueError("end_date must be provided if end_event_uri is provided")
        if self.end_date is not None and self.end_event_uri is None:
            raise ValueError("end_event_uri must be provided if end_date is provided")
        return self
    
    def to_dict_csv_export(self) -> dict[str, Any]:
        return {
            'uri': self.uri,
            'insee_code': self.insee_code,
            'label': self.label,
            'article_code': self.article_code,
            'start_event_uri': self.start_event_uri,
            'end_event_uri': self.end_event_uri,
            'start_date': self.start_date.isoformat(),
            'end_date': self.end_date.isoformat() if isinstance(self.end_date, datetime.date) else None,
            'start_date_count': 1,
            'end_date_count': 1 if isinstance(self.end_date, datetime.date) else 0 
        }

class InseeDistrictAddOrReplace(InseeGeoAddOrReplace):
    uri : str
    insee_code : str
    label: str
    article_code: str
    start_event_uri: str
    end_event_uri: Optional[str] = None
    start_date: datetime.date
    end_date: Optional[datetime.date] = None

    @model_validator(mode="after")
    def check_uri(self):
        check_uri(uri=self.uri)
        return self
    
    @model_validator(mode="after")
    def check_insee_code(self):
        check_insee_code(insee_code=self.insee_code)
        return self
    
    @model_validator(mode="after")
    def check_article_code(self):
        check_article_code(article_code=self.article_code)
        return self

    @model_validator(mode="after")
    def check_date(self):
        if self.end_date is not None and self.end_date < self.start_date:
            raise ValueError(f"Invalid date: {self.end_date} < {self.start_date}")
        return self
    
    @model_validator(mode="after")
    def check_start_event_uri(self):
        check_uri(uri=self.start_event_uri)
        return self
    
    @model_validator(mode="after")
    def check_end_event_uri(self):
        if self.end_event_uri is not None:
            check_uri(uri=self.end_event_uri)
        return self
    
    @model_validator(mode="after")
    def check_end_consistency(self):
        if self.end_event_uri is not None and self.end_date is None:
            raise ValueError("end_date must be provided if end_event_uri is provided")
        if self.end_date is not None and self.end_event_uri is None:
            raise ValueError("end_event_uri must be provided if end_date is provided")
        return self
    
    def to_dict_csv_export(self) -> dict[str, Any]:
        return {
            'uri': self.uri,
            'insee_code': self.insee_code,
            'label': self.label,
            'article_code': self.article_code,
            'start_event_uri': self.start_event_uri,
            'end_event_uri': self.end_event_uri,
            'start_date': self.start_date.isoformat(),
            'end_date': self.end_date.isoformat() if isinstance(self.end_date, datetime.date) else None,
            'start_date_count': 1,
            'end_date_count': 1 if isinstance(self.end_date, datetime.date) else 0 
        }

class InseeCollectiviteOutremerAddOrReplace(InseeGeoAddOrReplace):
    uri : str
    insee_code : str
    label: str
    article_code: str
    start_event_uri: str
    end_event_uri: Optional[str] = None
    start_date: datetime.date
    end_date: Optional[datetime.date] = None

    @model_validator(mode="after")
    def check_uri(self):
        check_uri(uri=self.uri)
        return self
    
    @model_validator(mode="after")
    def check_insee_code(self):
        check_insee_code(insee_code=self.insee_code)
        return self
    
    @model_validator(mode="after")
    def check_article_code(self):
        check_article_code(article_code=self.article_code)
        return self

    @model_validator(mode="after")
    def check_date(self):
        if self.end_date is not None and self.end_date < self.start_date:
            raise ValueError(f"Invalid date: {self.end_date} < {self.start_date}")
        return self
    
    @model_validator(mode="after")
    def check_start_event_uri(self):
        check_uri(uri=self.start_event_uri)
        return self
    
    @model_validator(mode="after")
    def check_end_event_uri(self):
        if self.end_event_uri is not None:
            check_uri(uri=self.end_event_uri)
        return self
    
    @model_validator(mode="after")
    def check_end_consistency(self):
        if self.end_event_uri is not None and self.end_date is None:
            raise ValueError("end_date must be provided if end_event_uri is provided")
        if self.end_date is not None and self.end_event_uri is None:
            raise ValueError("end_event_uri must be provided if end_date is provided")
        return self
    
    def to_dict_csv_export(self) -> dict[str, Any]:
        return {
            'uri': self.uri,
            'insee_code': self.insee_code,
            'label': self.label,
            'article_code': self.article_code,
            'start_event_uri': self.start_event_uri,
            'end_event_uri': self.end_event_uri,
            'start_date': self.start_date.isoformat(),
            'end_date': self.end_date.isoformat() if isinstance(self.end_date, datetime.date) else None,
            'start_date_count': 1,
            'end_date_count': 1 if isinstance(self.end_date, datetime.date) else 0 
        }

class InseePaysAddOrReplace(InseeGeoAddOrReplace):
    uri : str
    insee_code : str
    label: str
    article_code: str
    long_label: str
    iso3166alpha2_code: Optional[str]
    iso3166alpha3_code: Optional[str]
    iso3166num_code: Optional[str]
    start_event_uri: str
    end_event_uri: Optional[str] = None
    start_date: datetime.date
    end_date: Optional[datetime.date] = None

    @model_validator(mode="after")
    def check_uri(self):
        check_uri(uri=self.uri)
        return self
    
    @model_validator(mode="after")
    def check_insee_code(self):
        check_insee_code(insee_code=self.insee_code)
        return self
    
    @model_validator(mode="after")
    def check_article_code(self):
        check_article_code(article_code=self.article_code)
        return self

    @model_validator(mode="after")
    def check_date(self):
        if self.end_date is not None and self.end_date < self.start_date:
            raise ValueError(f"Invalid date: {self.end_date} < {self.start_date}")
        return self
    
    @model_validator(mode="after")
    def check_start_event_uri(self):
        check_uri(uri=self.start_event_uri)
        return self
    
    @model_validator(mode="after")
    def check_end_event_uri(self):
        if self.end_event_uri is not None:
            check_uri(uri=self.end_event_uri)
        return self
    
    @model_validator(mode="after")
    def check_end_consistency(self):
        if self.end_event_uri is not None and self.end_date is None:
            raise ValueError("end_date must be provided if end_event_uri is provided")
        if self.end_date is not None and self.end_event_uri is None:
            raise ValueError("end_event_uri must be provided if end_date is provided")
        return self
    
    def to_dict_csv_export(self) -> dict[str, Any]:
        return {
            'uri': self.uri,
            'insee_code': self.insee_code,
            'label': self.label,
            'article_code': self.article_code,
            'long_label': self.long_label,
            'iso3166alpha2_code': self.iso3166alpha2_code,
            'iso3166alpha3_code': self.iso3166alpha3_code,
            'iso3166num_code': self.iso3166num_code,
            'start_event_uri': self.start_event_uri,
            'end_event_uri': self.end_event_uri,
            'start_date': self.start_date.isoformat(),
            'end_date': self.end_date.isoformat() if isinstance(self.end_date, datetime.date) else None,
            'start_date_count': 1,
            'end_date_count': 1 if isinstance(self.end_date, datetime.date) else 0 
        }

class InseeGeoRemove(BaseModel):
    uri : str
    
    @model_validator(mode="after")
    def check_uri(self):
        check_uri(uri=self.uri)
        return self