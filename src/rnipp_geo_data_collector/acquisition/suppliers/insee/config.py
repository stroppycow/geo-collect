from typing import Union, List
from pydantic import BaseModel, RootModel, model_validator
from collections import Counter

from .checks.error_handler import ErrorHandlerRemoveBadArticleCodeAfterDownloadInseeCog, ErrorHandlerReplaceBadArticleCodeAfterDownloadInseeCog
from .checks.error_handler import ErrorHandlerRemoveInconsistentDateAfterDownloadInseeCog, ErrorHandlerReplaceInconsistentDateAfterDownloadInseeCog
from .checks.error_handler import ErrorHandlerRemoveBadInseeCodeAfterDownloadInseeCog, ErrorHandlerReplaceBadInseeCodeAfterDownloadInseeCog
from .checks.error_handler import ErrorHandlerRemoveBadInseeCodeOverlapAfterDownloadInseeCog, ErrorHandlerReplaceBadInseeCodeOverlapAfterDownloadInseeCog
from .checks.error_handler import ErrorHandlerRemoveBadStartDateAfterDownloadInseeCog, ErrorHandlerReplaceBadStartDateAfterDownloadInseeCog
from .checks.error_handler import ErrorHandlerRemoveBadEndDateAfterDownloadInseeCog, ErrorHandlerReplaceBadEndDateAfterDownloadInseeCog
from .checks.error_handler import ErrorHandlerRemoveCheckURIAfterDownloadInseeCog, ErrorHandlerReplaceCheckURIAfterDownloadInseeCog
from .checks.error_handler import ErrorHandlerRemoveDuplicatedURIAfterDownloadInseeCog


def check_unique_uri(uris: List[str]):
    counter = Counter(uris)
    duplicates = [id_ for id_, count in counter.items() if count > 1]
    if duplicates:
        example = duplicates[0]
        raise ValueError(
            f"URIs must be unique, but the following URIs are duplicated: {duplicates}. For example, the URI '{example}' appears {counter[example]} times."
        )

class InseeExceptionsToIgnoreOrCorrectModel(BaseModel):
    pass

class URIFormatInseeExceptionsToIgnoreOrCorrect(RootModel):
    root: list[Union[ErrorHandlerReplaceCheckURIAfterDownloadInseeCog, ErrorHandlerRemoveCheckURIAfterDownloadInseeCog]] = []

    @model_validator(mode="after")
    def unique_uri(self):
        check_unique_uri([item.uri for item in self.root])
        return self

class URIsUnicityInseeExceptionsToIgnoreOrCorrect(RootModel):
    root: list[ErrorHandlerRemoveDuplicatedURIAfterDownloadInseeCog] = []

    @model_validator(mode="after")
    def unique_uri(self):
        check_unique_uri([item.uri for item in self.root])
        return self

class ArticleCodeInseeExceptionsToIgnoreOrCorrect(RootModel):
    root: list[Union[ErrorHandlerReplaceBadArticleCodeAfterDownloadInseeCog, ErrorHandlerRemoveBadArticleCodeAfterDownloadInseeCog]] = []

    @model_validator(mode="after")
    def unique_uri(self):
        check_unique_uri([item.uri for item in self.root])
        return self

class InseeCodeInseeExceptionsToIgnoreOrCorrect(RootModel):
    root: list[Union[ErrorHandlerReplaceBadInseeCodeAfterDownloadInseeCog, ErrorHandlerRemoveBadInseeCodeAfterDownloadInseeCog]] = []

    @model_validator(mode="after")
    def unique_uri(self):
        check_unique_uri([item.uri for item in self.root])
        return self

class StartDateInseeExceptionsToIgnoreOrCorrect(RootModel):
    root: list[Union[ErrorHandlerReplaceBadStartDateAfterDownloadInseeCog, ErrorHandlerRemoveBadStartDateAfterDownloadInseeCog]] = []

    @model_validator(mode="after")
    def unique_uri(self):
        check_unique_uri([item.uri for item in self.root])
        return self


class EndDateInseeExceptionsToIgnoreOrCorrect(RootModel):
    root: list[Union[ErrorHandlerReplaceBadEndDateAfterDownloadInseeCog, ErrorHandlerRemoveBadEndDateAfterDownloadInseeCog]] = []

    @model_validator(mode="after")
    def unique_uri(self):
        check_unique_uri([item.uri for item in self.root])
        return self

class DateConsistencyInseeExceptionsToIgnoreOrCorrect(RootModel):
    root: list[Union[ErrorHandlerReplaceInconsistentDateAfterDownloadInseeCog, ErrorHandlerRemoveInconsistentDateAfterDownloadInseeCog]] = []

    @model_validator(mode="after")
    def unique_uri(self):
        check_unique_uri([item.uri for item in self.root])
        return self

class InseeCodeOverlapInseeExceptionsToIgnoreOrCorrect(RootModel):
    root: list[Union[ErrorHandlerReplaceBadInseeCodeOverlapAfterDownloadInseeCog, ErrorHandlerRemoveBadInseeCodeOverlapAfterDownloadInseeCog]] = []

    @model_validator(mode="after")
    def unique_uri(self):
        check_unique_uri([item.uri for item in self.root])
        return self


class CommunesInseeExceptionsToIgnoreOrCorrect(InseeExceptionsToIgnoreOrCorrectModel):
    uri_format: URIFormatInseeExceptionsToIgnoreOrCorrect = URIFormatInseeExceptionsToIgnoreOrCorrect()
    uri_unicity: URIsUnicityInseeExceptionsToIgnoreOrCorrect = URIsUnicityInseeExceptionsToIgnoreOrCorrect()
    article_code: ArticleCodeInseeExceptionsToIgnoreOrCorrect = ArticleCodeInseeExceptionsToIgnoreOrCorrect()
    insee_code: InseeCodeInseeExceptionsToIgnoreOrCorrect = InseeCodeInseeExceptionsToIgnoreOrCorrect()
    start_date: StartDateInseeExceptionsToIgnoreOrCorrect = StartDateInseeExceptionsToIgnoreOrCorrect()
    end_date: EndDateInseeExceptionsToIgnoreOrCorrect = EndDateInseeExceptionsToIgnoreOrCorrect()
    date_consistency: DateConsistencyInseeExceptionsToIgnoreOrCorrect = DateConsistencyInseeExceptionsToIgnoreOrCorrect()
    insee_code_overlap: InseeCodeOverlapInseeExceptionsToIgnoreOrCorrect = InseeCodeOverlapInseeExceptionsToIgnoreOrCorrect()

class ArrondissementsMunicipauxInseeExceptionsToIgnoreOrCorrect(InseeExceptionsToIgnoreOrCorrectModel):
    uri_format: URIFormatInseeExceptionsToIgnoreOrCorrect = URIFormatInseeExceptionsToIgnoreOrCorrect()
    uri_unicity: URIsUnicityInseeExceptionsToIgnoreOrCorrect = URIsUnicityInseeExceptionsToIgnoreOrCorrect()
    article_code: ArticleCodeInseeExceptionsToIgnoreOrCorrect = ArticleCodeInseeExceptionsToIgnoreOrCorrect()
    insee_code: InseeCodeInseeExceptionsToIgnoreOrCorrect = InseeCodeInseeExceptionsToIgnoreOrCorrect()
    start_date: StartDateInseeExceptionsToIgnoreOrCorrect = StartDateInseeExceptionsToIgnoreOrCorrect()
    end_date: EndDateInseeExceptionsToIgnoreOrCorrect = EndDateInseeExceptionsToIgnoreOrCorrect()
    date_consistency: DateConsistencyInseeExceptionsToIgnoreOrCorrect = DateConsistencyInseeExceptionsToIgnoreOrCorrect()
    insee_code_overlap: InseeCodeOverlapInseeExceptionsToIgnoreOrCorrect = InseeCodeOverlapInseeExceptionsToIgnoreOrCorrect()

class CollectivitesDOutreMerInseeExceptionsToIgnoreOrCorrect(InseeExceptionsToIgnoreOrCorrectModel):
    uri_format: URIFormatInseeExceptionsToIgnoreOrCorrect = URIFormatInseeExceptionsToIgnoreOrCorrect()
    uri_unicity: URIsUnicityInseeExceptionsToIgnoreOrCorrect = URIsUnicityInseeExceptionsToIgnoreOrCorrect()
    article_code: ArticleCodeInseeExceptionsToIgnoreOrCorrect = ArticleCodeInseeExceptionsToIgnoreOrCorrect()
    insee_code: InseeCodeInseeExceptionsToIgnoreOrCorrect = InseeCodeInseeExceptionsToIgnoreOrCorrect()
    start_date: StartDateInseeExceptionsToIgnoreOrCorrect = StartDateInseeExceptionsToIgnoreOrCorrect()
    end_date: EndDateInseeExceptionsToIgnoreOrCorrect = EndDateInseeExceptionsToIgnoreOrCorrect()
    date_consistency: DateConsistencyInseeExceptionsToIgnoreOrCorrect = DateConsistencyInseeExceptionsToIgnoreOrCorrect()
    insee_code_overlap: InseeCodeOverlapInseeExceptionsToIgnoreOrCorrect = InseeCodeOverlapInseeExceptionsToIgnoreOrCorrect()

class DistrictsInseeExceptionsToIgnoreOrCorrect(InseeExceptionsToIgnoreOrCorrectModel):
    uri_format: URIFormatInseeExceptionsToIgnoreOrCorrect = URIFormatInseeExceptionsToIgnoreOrCorrect()
    uri_unicity: URIsUnicityInseeExceptionsToIgnoreOrCorrect = URIsUnicityInseeExceptionsToIgnoreOrCorrect()
    article_code: ArticleCodeInseeExceptionsToIgnoreOrCorrect = ArticleCodeInseeExceptionsToIgnoreOrCorrect()
    insee_code: InseeCodeInseeExceptionsToIgnoreOrCorrect = InseeCodeInseeExceptionsToIgnoreOrCorrect()
    start_date: StartDateInseeExceptionsToIgnoreOrCorrect = StartDateInseeExceptionsToIgnoreOrCorrect()
    end_date: EndDateInseeExceptionsToIgnoreOrCorrect = EndDateInseeExceptionsToIgnoreOrCorrect()
    date_consistency: DateConsistencyInseeExceptionsToIgnoreOrCorrect = DateConsistencyInseeExceptionsToIgnoreOrCorrect()
    insee_code_overlap: InseeCodeOverlapInseeExceptionsToIgnoreOrCorrect = InseeCodeOverlapInseeExceptionsToIgnoreOrCorrect()

class PaysInseeExceptionsToIgnoreOrCorrect(InseeExceptionsToIgnoreOrCorrectModel):
    uri_format: URIFormatInseeExceptionsToIgnoreOrCorrect = URIFormatInseeExceptionsToIgnoreOrCorrect()
    uri_unicity: URIsUnicityInseeExceptionsToIgnoreOrCorrect = URIsUnicityInseeExceptionsToIgnoreOrCorrect()
    article_code: ArticleCodeInseeExceptionsToIgnoreOrCorrect = ArticleCodeInseeExceptionsToIgnoreOrCorrect()
    insee_code: InseeCodeInseeExceptionsToIgnoreOrCorrect = InseeCodeInseeExceptionsToIgnoreOrCorrect()
    start_date: StartDateInseeExceptionsToIgnoreOrCorrect = StartDateInseeExceptionsToIgnoreOrCorrect()
    end_date: EndDateInseeExceptionsToIgnoreOrCorrect = EndDateInseeExceptionsToIgnoreOrCorrect()
    date_consistency: DateConsistencyInseeExceptionsToIgnoreOrCorrect = DateConsistencyInseeExceptionsToIgnoreOrCorrect()
    insee_code_overlap: InseeCodeOverlapInseeExceptionsToIgnoreOrCorrect = InseeCodeOverlapInseeExceptionsToIgnoreOrCorrect()

class EventsInseeExceptionsToIgnoreOrCorrect(InseeExceptionsToIgnoreOrCorrectModel):
    uri_format: URIFormatInseeExceptionsToIgnoreOrCorrect = URIFormatInseeExceptionsToIgnoreOrCorrect()
    uri_unicity: URIsUnicityInseeExceptionsToIgnoreOrCorrect = URIsUnicityInseeExceptionsToIgnoreOrCorrect()

class InseeExceptionsToIgnoreOrCorrect(BaseModel):
    communes: CommunesInseeExceptionsToIgnoreOrCorrect = CommunesInseeExceptionsToIgnoreOrCorrect()
    arrondissements_municipaux: ArrondissementsMunicipauxInseeExceptionsToIgnoreOrCorrect = ArrondissementsMunicipauxInseeExceptionsToIgnoreOrCorrect()
    collectivites_outremer: CollectivitesDOutreMerInseeExceptionsToIgnoreOrCorrect = CollectivitesDOutreMerInseeExceptionsToIgnoreOrCorrect()
    districts: DistrictsInseeExceptionsToIgnoreOrCorrect = DistrictsInseeExceptionsToIgnoreOrCorrect()
    pays: PaysInseeExceptionsToIgnoreOrCorrect = PaysInseeExceptionsToIgnoreOrCorrect()
    events : EventsInseeExceptionsToIgnoreOrCorrect = EventsInseeExceptionsToIgnoreOrCorrect()

class InseeSupplierConfig(BaseModel):
    endpoint_url: str = "http://rdf.insee.fr/sparql"
    backoff_factor: float = 0.5
    max_retries: int = 5
    connect_timeout: float = 3
    read_timeout: float = 15
