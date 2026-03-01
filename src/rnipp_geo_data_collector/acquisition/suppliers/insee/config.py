from typing import Union, List
from pydantic import BaseModel, RootModel, model_validator
from collections import Counter

from .checks.apply_update import InseeCommuneAddOrReplace, InseeArrondissementMunicipalAddOrReplace, InseeDepartementAddOrReplace, InseeCollectiviteOutremerAddOrReplace, InseeDistrictAddOrReplace, InseePaysAddOrReplace, InseeGeoRemove

def check_unique_uri(uris: List[str]):
    counter = Counter(uris)
    duplicates = [id_ for id_, count in counter.items() if count > 1]
    if duplicates:
        example = duplicates[0]
        raise ValueError(
            f"URIs must be unique, but the following URIs are duplicated: {duplicates}. For example, the URI '{example}' appears {counter[example]} times."
        )

class InseeExceptionsToIgnoreOrCorrectModel(RootModel):
    pass


class CommunesInseeExceptionsToIgnoreOrCorrect(InseeExceptionsToIgnoreOrCorrectModel):
    root: list[Union[InseeCommuneAddOrReplace, InseeGeoRemove]] = []

    @model_validator(mode="after")
    def unique_uri(self):
        check_unique_uri([item.uri for item in self.root if isinstance(item, InseeCommuneAddOrReplace)])
        check_unique_uri([item.uri for item in self.root if isinstance(item, InseeGeoRemove)])
        return self


class ArrondissementsMunicipauxInseeExceptionsToIgnoreOrCorrect(InseeExceptionsToIgnoreOrCorrectModel):
    root: list[Union[InseeArrondissementMunicipalAddOrReplace, InseeGeoRemove]] = []

    @model_validator(mode="after")
    def unique_uri(self):
        check_unique_uri([item.uri for item in self.root if isinstance(item, InseeArrondissementMunicipalAddOrReplace)])
        check_unique_uri([item.uri for item in self.root if isinstance(item, InseeGeoRemove)])
        return self
    
class DepartementsInseeExceptionsToIgnoreOrCorrect(InseeExceptionsToIgnoreOrCorrectModel):
    root: list[Union[InseeDepartementAddOrReplace, InseeGeoRemove]] = []

    @model_validator(mode="after")
    def unique_uri(self):
        check_unique_uri([item.uri for item in self.root if isinstance(item, InseeDepartementAddOrReplace)])
        check_unique_uri([item.uri for item in self.root if isinstance(item, InseeGeoRemove)])
        return self

class CollectivitesDOutreMerInseeExceptionsToIgnoreOrCorrect(InseeExceptionsToIgnoreOrCorrectModel):
    root: list[Union[InseeCollectiviteOutremerAddOrReplace, InseeGeoRemove]] = []

    @model_validator(mode="after")
    def unique_uri(self):
        check_unique_uri([item.uri for item in self.root if isinstance(item, InseeCollectiviteOutremerAddOrReplace)])
        check_unique_uri([item.uri for item in self.root if isinstance(item, InseeGeoRemove)])
        return self


class DistrictsInseeExceptionsToIgnoreOrCorrect(InseeExceptionsToIgnoreOrCorrectModel):
    root: list[Union[InseeDistrictAddOrReplace, InseeGeoRemove]] = []

    @model_validator(mode="after")
    def unique_uri(self):
        check_unique_uri([item.uri for item in self.root if isinstance(item, InseeDistrictAddOrReplace)])
        check_unique_uri([item.uri for item in self.root if isinstance(item, InseeGeoRemove)])
        return self


class PaysInseeExceptionsToIgnoreOrCorrect(InseeExceptionsToIgnoreOrCorrectModel):
    root: list[Union[InseePaysAddOrReplace, InseeGeoRemove]] = []

    @model_validator(mode="after")
    def unique_uri(self):
        check_unique_uri([item.uri for item in self.root if isinstance(item, InseePaysAddOrReplace)])
        check_unique_uri([item.uri for item in self.root if isinstance(item, InseeGeoRemove)])
        return self


class InseeExceptionsToIgnoreOrCorrect(BaseModel):
    communes: CommunesInseeExceptionsToIgnoreOrCorrect = CommunesInseeExceptionsToIgnoreOrCorrect()
    arrondissements_municipaux: ArrondissementsMunicipauxInseeExceptionsToIgnoreOrCorrect = ArrondissementsMunicipauxInseeExceptionsToIgnoreOrCorrect()
    departements: DepartementsInseeExceptionsToIgnoreOrCorrect = DepartementsInseeExceptionsToIgnoreOrCorrect()
    collectivites_outremer: CollectivitesDOutreMerInseeExceptionsToIgnoreOrCorrect = CollectivitesDOutreMerInseeExceptionsToIgnoreOrCorrect()
    districts: DistrictsInseeExceptionsToIgnoreOrCorrect = DistrictsInseeExceptionsToIgnoreOrCorrect()
    pays: PaysInseeExceptionsToIgnoreOrCorrect = PaysInseeExceptionsToIgnoreOrCorrect()

class InseeSupplierConfig(BaseModel):
    endpoint_url: str = "http://rdf.insee.fr/sparql"
    backoff_factor: float = 0.5
    max_retries: int = 5
    connect_timeout: float = 3
    read_timeout: float = 15
