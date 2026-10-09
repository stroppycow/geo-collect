import csv
import logging
import re
import unicodedata
from pathlib import Path
from typing import NamedTuple

import duckdb
import igraph
from rapidfuzz import fuzz

from ..graph.graph_event import build_history_graph_of_geo_entities
from ..metadata import (
    GeoCSVFileMetadata,
    InfoCurrentStoredData,
    StringColumnDataType,
    StringListColumnDataType,
)

logger = logging.getLogger(__name__)

GEO_ENTITY_URI_PREFIX = "http://id.insee.fr/geo/"


def get_uri(vertex: igraph.Vertex, uri_attr: str = "uri") -> str:
    uri = vertex.attributes().get(uri_attr)
    if isinstance(uri, str):
        return uri
    else:
        raise TypeError("The uri is not a string")


def get_parent_uri(
    vertex: igraph.Vertex, parent_uri_attr: str = "parent_uri", sep: str = "|"
) -> list[str]:
    parent_uri = vertex.attributes().get(parent_uri_attr)
    if isinstance(parent_uri, str):
        return parent_uri.split(sep=sep)
    else:
        raise TypeError("The parent_uri_attr is not a string")


def entity_type_from_uri(uri: str) -> str:
    return re.sub(
        pattern=r"[/].+",
        repl="",
        string=uri.replace(GEO_ENTITY_URI_PREFIX, ""),
    )


def _geo_entity_type(vertex: igraph.Vertex) -> str | None:
    uri = vertex.attributes().get("uri")
    if isinstance(uri, str):
        return entity_type_from_uri(uri)
    else:
        return None


def normalize_location_label(s: str) -> str:
    STOPWORDS = {
        "territoire",
        "departement",
        "l",
        "le",
        "la",
        "de",
        "d",
        "a",
        "les",
        "en",
        "au",
        "aux",
        "une",
        "sur",
        "du",
    }
    s = s.lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.replace("-", " ")
    s = s.replace("'", " ")
    s = s.replace("&", " ")
    s = s.replace(";", " ")
    s = re.sub(r"[^a-z ]", "", s)
    s = re.sub(r"[ ]+", " ", s)
    words = s.split()
    words = [w for w in words if w not in STOPWORDS]
    return " ".join(words)


def is_label_location_similar(s1: str, s2: str) -> bool:
    try:
        score = fuzz.ratio(normalize_location_label(s1), normalize_location_label(s2))
        if score >= 90:
            return True
    except (AttributeError, TypeError) as e:
        logger.debug("Unable to compare the location labels %r and %r: %s", s1, s2, e)
    return False


class LocationRule(NamedTuple):
    insee_codes: tuple[str, ...]
    label: str
    geo_entity_type: str | None = None
    not_france_metropolitaine: bool = False


LOCATION_RULES: dict[str, tuple[LocationRule, ...]] = {
    "algerie": (
        LocationRule(
            insee_codes=("91", "91000", "91352", "99352"),
            label="Alger",
            not_france_metropolitaine=True,
        ),
        LocationRule(
            insee_codes=("92", "92000", "92352", "99352"),
            label="Oran",
            not_france_metropolitaine=True,
        ),
        LocationRule(
            insee_codes=("93", "93000", "93352", "99352"),
            label="Constantine",
            not_france_metropolitaine=True,
        ),
        LocationRule(
            insee_codes=("94", "94000", "94352", "99352"),
            label="Territoires du sud de lAlgérie",
            not_france_metropolitaine=True,
        ),
    ),
    "maroc": (
        LocationRule(
            insee_codes=("95", "95000", "95350", "99350"),
            label="Maroc",
            geo_entity_type="collectiviteDOutreMer",
        ),
    ),
    "tunisie": (
        LocationRule(
            insee_codes=("96", "96000", "96351", "99351"),
            label="Tunisie",
            geo_entity_type="collectiviteDOutreMer",
        ),
    ),
    "guadeloupe": (
        LocationRule(insee_codes=("98702", "97100", "971"), label="Guadeloupe"),
    ),
    "martinique": (
        LocationRule(insee_codes=("98703", "97200", "972"), label="Martinique"),
    ),
    "guyane": (LocationRule(insee_codes=("98704", "97300", "973"), label="Guyane"),),
    "reunion": (LocationRule(insee_codes=("98405", "97400", "974"), label="Réunion"),),
    "mayotte": (
        LocationRule(
            insee_codes=("976", "97600", "985", "98500", "98402"), label="Mayotte"
        ),
    ),
    "saint_pierre_et_miquelon": (
        LocationRule(
            insee_codes=("975", "97500", "98701"),
            label="Saint-Pierre-et-Miquelon",
        ),
    ),
    "saint_barthelemy": (
        LocationRule(insee_codes=("977", "97700"), label="Saint-Barthélemy"),
    ),
    "saint_martin": (LocationRule(insee_codes=("978", "97800"), label="Saint-Martin"),),
    "polynesie_française": (
        LocationRule(insee_codes=("987", "98700"), label="Polynésie française"),
    ),
    "nouvelle_caledonie": (
        LocationRule(insee_codes=("988", "98800"), label="Nouvelle-Calédonie"),
    ),
}


def _is_france_metropolitaine(parent_vertices: list[igraph.Vertex]) -> bool:
    for v in parent_vertices:
        is_france_metropolitaine = v.attributes().get("is_france_metropolitaine")
        if isinstance(is_france_metropolitaine, bool) and is_france_metropolitaine:
            return True
    return False


def _match_location_rule(vertex: igraph.Vertex, rule: LocationRule) -> bool:
    insee_code = vertex.attributes().get("insee_code")
    label = vertex.attributes().get("label")
    if not (isinstance(insee_code, str) and isinstance(label, str)):
        return False
    if (
        rule.geo_entity_type is not None
        and _geo_entity_type(vertex) != rule.geo_entity_type
    ):
        return False
    if rule.not_france_metropolitaine:
        is_france_metropolitaine = vertex.attributes().get("is_france_metropolitaine")
        if not (
            isinstance(is_france_metropolitaine, bool) and not is_france_metropolitaine
        ):
            return False
    return insee_code in rule.insee_codes and is_label_location_similar(
        label, rule.label
    )


def compute_location_typologies(
    parent_vertices: list[igraph.Vertex],
) -> dict[str, bool]:
    typology_dict: dict[str, bool] = {
        "france_metropolitaine": _is_france_metropolitaine(parent_vertices)
    }
    for name, rules in LOCATION_RULES.items():
        typology_dict[name] = any(
            _match_location_rule(v, rule) for v in parent_vertices for rule in rules
        )
    return typology_dict


def typologize_communes(
    duckdb_connection: duckdb.DuckDBPyConnection,
    current_stored_data: InfoCurrentStoredData,
    output_dir: Path,
) -> None:

    parent_uri_colname_label = "parent_uri"
    uri_colname_label = "uri"

    logger.info("Add column location_typology to the communes table.")
    communes_metadata = current_stored_data.get_data(key="insee_communes")
    communes_path = communes_metadata.path
    if isinstance(communes_metadata, GeoCSVFileMetadata):
        communes_colnames = communes_metadata.colnames
    else:
        raise TypeError(
            """
            The metadata for the communes table is not a GeoCSVFileMetadata
            """
        )

    graph = build_history_graph_of_geo_entities(
        duckdb_connection=duckdb_connection,
        current_stored_data=current_stored_data,
        tables=["insee_communes", "insee_departements", "insee_collectivites_outremer"],
    )

    parent_uri_colname = None
    uri_colname = None
    for x in communes_colnames:
        if x.name == parent_uri_colname_label:
            parent_uri_colname = x
        elif x.name == uri_colname_label:
            uri_colname = x
    if parent_uri_colname is None:
        raise TypeError(
            f"""
            The column '{parent_uri_colname_label}'
            is not found in the communes table.
            """
        )
    if not isinstance(parent_uri_colname, StringListColumnDataType):
        raise TypeError(
            f"The column '{parent_uri_colname_label}' is not a StringListColumnDataType"
        )

    if uri_colname is None:
        raise TypeError(
            f"""
            The column '{uri_colname_label}'
            is not found in the communes table.
            """
        )
    if not isinstance(uri_colname, StringColumnDataType):
        raise TypeError(f"The column '{uri_colname}' is not a StringColumnDataType")

    location_category_dict: dict[str, tuple[str, igraph.Vertex]] = {}
    for v in graph.vs:
        uri = get_uri(vertex=v, uri_attr=uri_colname.name)
        entity_type = entity_type_from_uri(uri)
        if entity_type == "commune":
            try:
                parent_uri_list = get_parent_uri(
                    vertex=v,
                    parent_uri_attr=parent_uri_colname.name,
                    sep=parent_uri_colname.sep,
                )
            except Exception as e:
                raise RuntimeError(
                    f"Unable to get the parent_uri for the uri {uri}"
                ) from e

            parent_vertices = []
            for parent_uri in parent_uri_list:
                try:
                    parent_vertices.append(graph.vs.find(name=parent_uri))
                except Exception as e:
                    raise RuntimeError(
                        f"""
                        Unable to find the parent vertex
                        for the uri {uri} with parent_uri={parent_uri}
                        """
                    ) from e

            typology_dict = compute_location_typologies(parent_vertices)
            true_items = [k for k, v in typology_dict.items() if v]
            if len(true_items) == 0:
                raise RuntimeError(
                    f"""
                    Unable to compute the location category
                    for the commune with the URI {uri} : {typology_dict}
                    """
                )
            elif len(true_items) > 1:
                raise RuntimeError(
                    f"""
                    Several location categories may apply
                    for the commune with the URI {uri} : {typology_dict}
                    """
                )
            else:
                location_category_dict[uri] = (true_items[0], v)

    output_csv_path = output_dir / communes_path.name
    new_col = StringColumnDataType(name="location_typology")
    output_dir.mkdir(parents=True, exist_ok=True)
    with open(output_csv_path, mode="w", newline="", encoding="utf-8") as f_communes:
        writer_communes = csv.DictWriter(
            f_communes, fieldnames=[x.name for x in communes_colnames] + [new_col.name]
        )
        writer_communes.writeheader()
        for el in location_category_dict.values():
            v = el[1]
            row_output = {col.name: v[col.name] for col in communes_colnames}
            row_output[new_col.name] = el[0]
            writer_communes.writerow(row_output)
    current_stored_data.replace_data(
        key="insee_communes",
        value=GeoCSVFileMetadata(
            path=output_csv_path,
            header=True,
            delim=",",
            encoding="utf-8",
            colnames=communes_colnames + [new_col],
        ),
    )
    logger.info("The column location_typology has been added to the communes table.")
