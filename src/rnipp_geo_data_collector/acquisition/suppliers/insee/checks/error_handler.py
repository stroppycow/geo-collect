import datetime
from pydantic import BaseModel

class ErrorHandlerReplaceCheckURIAfterDownloadInseeCog(BaseModel):
    uri: str
    replace: str

class ErrorHandlerRemoveCheckURIAfterDownloadInseeCog(BaseModel):
    uri: str

class ErrorHandlerRemoveDuplicatedURIAfterDownloadInseeCog(BaseModel):
    uri: str

class ErrorHandlerRemoveBadInseeCodeAfterDownloadInseeCog(BaseModel):
    uri: str
        
class ErrorHandlerReplaceBadInseeCodeAfterDownloadInseeCog(BaseModel):
    uri: str
    insee_code: str

class ErrorHandlerRemoveBadArticleCodeAfterDownloadInseeCog(BaseModel):
    uri: str
        
class ErrorHandlerReplaceBadArticleCodeAfterDownloadInseeCog(BaseModel):
    uri: str
    article_code: str

class ErrorHandlerRemoveBadStartDateAfterDownloadInseeCog(BaseModel):
    uri: str

class ErrorHandlerReplaceBadStartDateAfterDownloadInseeCog(BaseModel):
    uri: str
    start_date: datetime.date

class ErrorHandlerRemoveBadEndDateAfterDownloadInseeCog(BaseModel):
    uri: str

class ErrorHandlerReplaceBadEndDateAfterDownloadInseeCog(BaseModel):
    uri: str
    end_date: datetime.date

class ErrorHandlerRemoveInconsistentDateAfterDownloadInseeCog(BaseModel):
    uri: str

class ErrorHandlerReplaceInconsistentDateAfterDownloadInseeCog(BaseModel):
    uri: str
    start_date: datetime.date
    end_date: datetime.date

class ErrorHandlerRemoveBadInseeCodeOverlapAfterDownloadInseeCog(BaseModel):
    uri: str

class ErrorHandlerReplaceBadInseeCodeOverlapAfterDownloadInseeCog(BaseModel):
    uri: str
    insee_code: str
    start_date: datetime.date
    end_date: datetime.date
