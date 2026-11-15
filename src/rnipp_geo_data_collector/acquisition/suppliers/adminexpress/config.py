import re
from collections import Counter
from typing import Any

from pydantic import BaseModel, RootModel, model_validator


class AdminExpressSupplierConfig(BaseModel):
    """Connection parameters for the IGN Admin Express COG download catalog.

    The catalog is the Géoplateforme (data.geopf.fr) Atom feed of the
    ADMIN-EXPRESS-COG product: the latest edition matching `zone` and
    `format` is resolved from it at acquisition time, so no edition date is
    hardcoded. Each layer is then downloaded as a GeoParquet file.
    """

    catalog_url: str = "https://data.geopf.fr/telechargement/resource/ADMIN-EXPRESS-COG"
    zone: str = "FRA"
    format: str = "GeoParquet"
    backoff_factor: float = 0.5
    max_retries: int = 5
    connect_timeout: float = 20
    read_timeout: float = 600


def check_cleabs(cleabs: str):
    if re.fullmatch(pattern=r"[A-Z0-9_]+", string=cleabs) is None:
        raise ValueError(f"Invalid cleabs: {cleabs}")


def check_unique_cleabs(cleabs: list[str]):
    counter = Counter(cleabs)
    duplicates = [id_ for id_, count in counter.items() if count > 1]
    if duplicates:
        example = duplicates[0]
        raise ValueError(
            f"""
            cleabs must be unique, but the following cleabs are duplicated :
            {duplicates}. For example, the cleabs '{example}'
            appears {counter[example]} times.
            """
        )


class AdminExpressGeoAddOrReplace(BaseModel):
    """Entity to add or replace in an Admin Express COG layer, keyed by `cleabs`.

    `fields` holds the layer attribute columns as a mapping
    (column name -> value, all exported as strings); `geometrie_wkt` is the
    contour as WKT, parsed with `ST_GeomFromText` when the correction is
    applied.
    """

    cleabs: str
    fields: dict[str, str] = {}
    geometrie_wkt: str | None = None

    @model_validator(mode="after")
    def check_cleabs(self):
        check_cleabs(cleabs=self.cleabs)
        return self

    def to_dict_csv_export(self) -> dict[str, Any]:
        row: dict[str, Any] = dict(self.fields)
        row["cleabs"] = self.cleabs
        row["geometrie"] = self.geometrie_wkt
        return row


class AdminExpressGeoRemove(BaseModel):
    cleabs: str

    @model_validator(mode="after")
    def check_cleabs(self):
        check_cleabs(cleabs=self.cleabs)
        return self


class AdminExpressLayerExceptionsToIgnoreOrCorrect(RootModel):
    root: list[AdminExpressGeoAddOrReplace | AdminExpressGeoRemove] = []

    @model_validator(mode="after")
    def unique_cleabs(self):
        check_unique_cleabs(
            [
                item.cleabs
                for item in self.root
                if isinstance(item, AdminExpressGeoAddOrReplace)
            ]
        )
        check_unique_cleabs(
            [
                item.cleabs
                for item in self.root
                if isinstance(item, AdminExpressGeoRemove)
            ]
        )
        return self


class AdminExpressExceptionsToIgnoreOrCorrect(BaseModel):
    collectivites_territoriales: AdminExpressLayerExceptionsToIgnoreOrCorrect = (
        AdminExpressLayerExceptionsToIgnoreOrCorrect()
    )
    arrondissements_municipaux: AdminExpressLayerExceptionsToIgnoreOrCorrect = (
        AdminExpressLayerExceptionsToIgnoreOrCorrect()
    )
    departements: AdminExpressLayerExceptionsToIgnoreOrCorrect = (
        AdminExpressLayerExceptionsToIgnoreOrCorrect()
    )
    communes: AdminExpressLayerExceptionsToIgnoreOrCorrect = (
        AdminExpressLayerExceptionsToIgnoreOrCorrect()
    )
    communes_associees_ou_deleguees: AdminExpressLayerExceptionsToIgnoreOrCorrect = (
        AdminExpressLayerExceptionsToIgnoreOrCorrect()
    )
