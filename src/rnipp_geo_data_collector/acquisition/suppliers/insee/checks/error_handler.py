import datetime
from typing import Optional
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
    
def check_article_code(article_code: str):
    if re.fullmatch(pattern = r"[0-9A-Za-z]*", string=article_code) is None:
        raise ValueError(f"Invalid Article code: {article_code}")
    
def check_uri_list(parent_uris: str):
    if parent_uris == "":
        raise ValueError("Parent URIs list cannot be empty")
    for uri in parent_uris.split("|"):
        check_uri(uri=uri)


class ErrorHandlerReplaceCheckURIAfterDownloadInseeCog(BaseModel):
    uri: str
    replace: str

    @model_validator(mode="after")
    def check_uri(self):
        check_uri(uri=self.uri)
        return self
    
    @model_validator(mode="after")
    def check_replace(self):
        check_uri(uri=self.replace)
        return self

class ErrorHandlerRemoveCheckURIAfterDownloadInseeCog(BaseModel):
    uri: str

    @model_validator(mode="after")
    def check_uri(self):
        check_uri(uri=self.uri)
        return self

class ErrorHandlerRemoveDuplicatedURIAfterDownloadInseeCog(BaseModel):
    uri: str

    @model_validator(mode="after")
    def check_uri(self):
        check_uri(uri=self.uri)
        return self

class ErrorHandlerRemoveBadInseeCodeAfterDownloadInseeCog(BaseModel):
    uri: str

    @model_validator(mode="after")
    def check_uri(self):
        check_uri(uri=self.uri)
        return self
        
class ErrorHandlerReplaceBadInseeCodeAfterDownloadInseeCog(BaseModel):
    uri: str
    insee_code: str

    @model_validator(mode="after")
    def check_uri(self):
        check_uri(uri=self.uri)
        return self

    @model_validator(mode="after")
    def check_insee_code(self):
        check_insee_code(insee_code=self.insee_code)
        return self

class ErrorHandlerRemoveBadArticleCodeAfterDownloadInseeCog(BaseModel):
    uri: str

    @model_validator(mode="after")
    def check_uri(self):
        check_uri(uri=self.uri)
        return self
        
class ErrorHandlerReplaceBadArticleCodeAfterDownloadInseeCog(BaseModel):
    uri: str
    article_code: str

    @model_validator(mode="after")
    def check_uri(self):
        check_uri(uri=self.uri)
        return self
    
    @model_validator(mode="after")
    def check_article_code(self):
        check_article_code(article_code=self.article_code)
        return self

class ErrorHandlerRemoveBadStartDateAfterDownloadInseeCog(BaseModel):
    uri: str

    @model_validator(mode="after")
    def check_uri(self):
        check_uri(uri=self.uri)
        return self
    

class ErrorHandlerReplaceBadStartDateAfterDownloadInseeCog(BaseModel):
    uri: str
    start_date: datetime.date

    @model_validator(mode="after")
    def check_uri(self):
        check_uri(uri=self.uri)
        return self
    

class ErrorHandlerRemoveBadEndDateAfterDownloadInseeCog(BaseModel):
    uri: str

    @model_validator(mode="after")
    def check_uri(self):
        check_uri(uri=self.uri)
        return self

class ErrorHandlerReplaceBadEndDateAfterDownloadInseeCog(BaseModel):
    uri: str
    end_date: Optional[datetime.date] = None

    @model_validator(mode="after")
    def check_uri(self):
        check_uri(uri=self.uri)
        return self

class ErrorHandlerRemoveInconsistentDateAfterDownloadInseeCog(BaseModel):
    uri: str

    @model_validator(mode="after")
    def check_uri(self):
        check_uri(uri=self.uri)
        return self

class ErrorHandlerReplaceInconsistentDateAfterDownloadInseeCog(BaseModel):
    uri: str
    start_date: datetime.date
    end_date: Optional[datetime.date] = None

    @model_validator(mode="after")
    def check_uri(self):
        check_uri(uri=self.uri)
        return self
    
    @model_validator(mode="after")
    def check_date(self):
        if self.end_date is not None and self.end_date < self.start_date:
            raise ValueError(f"Invalid date: {self.end_date} < {self.start_date}")
        return self

class ErrorHandlerRemoveBadInseeCodeOverlapAfterDownloadInseeCog(BaseModel):
    uri: str

    @model_validator(mode="after")
    def check_uri(self):
        check_uri(uri=self.uri)
        return self

class ErrorHandlerReplaceBadInseeCodeOverlapAfterDownloadInseeCog(BaseModel):
    uri: str
    insee_code: str
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
    def check_date(self):
        if self.end_date is not None and self.end_date < self.start_date:
            raise ValueError(f"Invalid date: {self.end_date} < {self.start_date}")
        return self

class ErrorHandlerRemoveParentDuplicatedAfterDownloadInseeCog(BaseModel):
    uri: str

    @model_validator(mode="after")
    def check_uri(self):
        check_uri(uri=self.uri)
        return self

class ErrorHandlerReplaceParentDuplicatedAfterDownloadInseeCog(BaseModel):
    uri: str
    parent_uris: str

    @model_validator(mode="after")
    def check_uri(self):
        check_uri(uri=self.uri)
        return self
    
    @model_validator(mode="after")
    def check_parent_code(self):
        check_uri_list(parent_uris=self.parent_uris)
        return self
