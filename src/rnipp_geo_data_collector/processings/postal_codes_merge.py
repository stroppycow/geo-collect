import csv
import logging
import re
import unicodedata
from pathlib import Path

import duckdb
import igraph
from rapidfuzz import fuzz

from ..graph.graph_event import build_history_graph_of_geo_entities
from ..metadata import (
    DateColumnDataType,
    GeoCSVFileMetadata,
    InfoCurrentStoredData,
    IntegerColumnDataType,
    StringColumnDataType,
    StringListColumnDataType,
)

logger = logging.getLogger(__name__)


def get_geo_entity_type(vertex, uri_attr: str = "uri") -> str:
    uri = vertex.attributes().get(uri_attr)
    if not (isinstance(uri, str)):
        raise TypeError("The uri is not a string")
    return re.sub(
        pattern=r"[/].+", repl="", string=uri.replace("http://id.insee.fr/geo/", "")
    )


def normalize_location_label(s: str, first_word_only: bool = False) -> str:
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
    REPLACE_WORDS = {"st": "saint", "ste": "sainte", "pt": "pont"}
    s = s.replace("œ", "oe")
    s = s.replace("Œ", "oe")
    s = s.lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.replace("-", " ")
    s = s.replace("'", " ")
    s = s.replace("&", " ")
    s = s.replace(";", " ")
    s = re.sub(r"[^a-z0-9 ]", "", s)
    s = re.sub(r"([0-9]+)(e|eme|er|ier) ", r"\1 ", s)
    words = s.split()
    words = [w for w in words if w not in STOPWORDS]
    words = [REPLACE_WORDS.get(w, w) for w in words]
    if len(words) > 1 and words[len(words) - 1] in ["arrondissement", "arrond", "arro"]:
        words.pop()

    if first_word_only:
        return words[0] if words else ""

    return " ".join(words)


def is_label_location_similar(s1: str, s2: str, first_word_only: bool = False) -> bool:
    try:
        score = fuzz.ratio(
            normalize_location_label(s1, first_word_only=first_word_only),
            normalize_location_label(s2, first_word_only=first_word_only),
        )
    except (AttributeError, TypeError, ValueError):
        return False
    return score >= 90


def get_postal_codes_current_geo_entity(
    vertex: igraph.Vertex,
    hexasmal_data_dict: dict[str, list[tuple[str, str, str, str]]],
    insee_colname_uri_label: str,
    insee_colname_label_label: str,
    insee_colname_insee_code_label: str,
    insee_colname_end_date_label: str,
) -> list[str]:
    uri = vertex.attributes().get(insee_colname_uri_label)
    insee_label = vertex.attributes().get(insee_colname_label_label)
    insee_code = vertex.attributes().get(insee_colname_insee_code_label)
    end_date = vertex.attributes().get(insee_colname_end_date_label)
    if not isinstance(uri, str):
        raise TypeError(f"The uri of the vertex is not a string. It is {type(uri)}")
    if (
        isinstance(insee_code, str)
        and isinstance(insee_label, str)
        and end_date is None
        and insee_code in hexasmal_data_dict
    ):
        laposte_labels: list[str] = []
        laposte_labels.extend(
            [x[0] for x in hexasmal_data_dict[insee_code] if len(x) > 0]
        )
        laposte_labels.extend(
            [x[2] for x in hexasmal_data_dict[insee_code] if len(x) > 2]
        )
        laposte_labels = list({x for x in laposte_labels if isinstance(x, str)})
        if len(laposte_labels) > 0:
            if re.search(pattern="[(]Canton d", string=insee_label) is not None:
                for laposte_label in laposte_labels:
                    if is_label_location_similar(
                        laposte_label, insee_label, first_word_only=True
                    ):
                        return list(
                            {
                                x[1]
                                for x in hexasmal_data_dict[insee_code]
                                if isinstance(x[1], str)
                            }
                        )
            else:
                for laposte_label in laposte_labels:
                    if is_label_location_similar(
                        laposte_label, insee_label, first_word_only=False
                    ):
                        return list(
                            {
                                x[1]
                                for x in hexasmal_data_dict[insee_code]
                                if isinstance(x[1], str)
                            }
                        )
    return []


def _label_similarity_score(s1: str, s2: str) -> float:
    """Similarity score (0-100) between two raw labels after normalisation."""
    return fuzz.ratio(
        normalize_location_label(s1),
        normalize_location_label(s2),
    )


def assign_postal_codes_by_hexasmal_labels(
    subgraph: igraph.Graph,
    hexasmal_data_dict: dict[str, list[tuple[str, str, str, str]]],
    postal_codes_by_vertex: dict[int, set[str]],
    insee_colname_label_label: str,
    insee_colname_insee_code_label: str,
) -> dict[str, set[int]]:
    """
    Match the Hexasmal entries of every INSEE code present in the component
    against the labels of the vertices that have no postal code yet.

    An entry with an ``associated_name`` is attributed to the best matching
    vertex (highest label similarity, closest to the source vertex on ties);
    such a postal code is then *dedicated* to that vertex and will not be
    propagated further up the graph. An entry without ``associated_name``
    (or whose ``associated_name`` matches no vertex of the component) is
    attributed to every vertex whose label matches the ``delivery_label``
    or the Hexasmal ``name``.

    Returns the mapping ``postal_code -> dedicated vertex indices``.
    """
    n = subgraph.vcount()
    labels = [subgraph.vs[i][insee_colname_label_label] for i in range(n)]
    targets = [i for i in range(n) if not postal_codes_by_vertex[i]]
    dedicated: dict[str, set[int]] = {}
    for source in range(n):
        insee_code = subgraph.vs[source][insee_colname_insee_code_label]
        for (
            name,
            postal_code,
            delivery_label,
            associated_name,
        ) in hexasmal_data_dict.get(insee_code, []):
            if not postal_code:
                continue
            matched = False
            if associated_name:
                scores = {
                    i: _label_similarity_score(labels[i], associated_name)
                    for i in targets
                    if i != source
                }
                candidates = {i: s for i, s in scores.items() if s >= 90.0}
                if candidates:
                    cand_list = list(candidates)
                    distances = subgraph.distances(source, cand_list, mode="ALL")[0]
                    winner = cand_list[
                        min(
                            range(len(cand_list)),
                            key=lambda k: (-candidates[cand_list[k]], distances[k]),
                        )
                    ]
                    postal_codes_by_vertex[winner].add(postal_code)
                    dedicated.setdefault(postal_code, set()).add(winner)
                    matched = True
            if not matched:
                for i in targets:
                    if i == source:
                        continue
                    if is_label_location_similar(
                        labels[i], delivery_label
                    ) or is_label_location_similar(labels[i], name):
                        postal_codes_by_vertex[i].add(postal_code)
    return dedicated


def propagate_postal_codes_in_component(
    subgraph: igraph.Graph,
    postal_codes_by_vertex: dict[int, set[str]],
    dedicated: dict[str, set[int]],
) -> None:
    """
    Propagate postal codes inside a connected component until fixpoint:

    - a vertex inherits the union of the postal codes of its predecessors
      (a merged entity covers the territories of the entities it comes from);
    - a vertex without any postal code and with a single successor inherits
      the successor's postal codes, except those dedicated through an
      ``associated_name`` to another vertex.
    """
    n = subgraph.vcount()
    changed = True
    while changed:
        changed = False
        for i in range(n):
            in_degree = subgraph.degree(i, mode="IN")
            inherited = set()
            for predecessor in subgraph.predecessors(i):
                inherited |= postal_codes_by_vertex[predecessor]
            # a merged entity (in-degree >= 2) covers the territories of all its
            # predecessors, even if it already has postal codes of its own
            if (
                in_degree >= 2 or not postal_codes_by_vertex[i]
            ) and inherited - postal_codes_by_vertex[i]:
                postal_codes_by_vertex[i] |= inherited
                changed = True
            if not postal_codes_by_vertex[i]:
                successors = subgraph.successors(i)
                successors_with_codes = [
                    s for s in successors if postal_codes_by_vertex[s]
                ]
                if len(successors) == 1 and successors_with_codes:
                    for postal_code in postal_codes_by_vertex[successors_with_codes[0]]:
                        if postal_code not in dedicated or i in dedicated[postal_code]:
                            postal_codes_by_vertex[i].add(postal_code)
                            changed = True


def assign_postal_codes_from_arrondissements(
    graph: igraph.Graph,
    insee_colname_uri_label: str,
    insee_colname_parent_uri_label: str,
    postal_codes_attribute: str,
) -> None:
    """
    Aggregate the postal codes of the municipal arrondissements into their
    parent commune (e.g. Paris, Lyon, Marseille): a commune whose INSEE code
    is absent from Hexasmal gets the union of its arrondissements' codes.
    """
    arrondissements_by_parent: dict[str, list[int]] = {}
    for v in graph.vs:
        uri = v[insee_colname_uri_label]
        if isinstance(uri, str) and "arrondissementMunicipal" in uri:
            parent_uri = v[insee_colname_parent_uri_label]
            if isinstance(parent_uri, str):
                for parent in parent_uri.split("|"):
                    if parent:
                        arrondissements_by_parent.setdefault(parent, []).append(v.index)
    for v in graph.vs:
        uri = v[insee_colname_uri_label]
        if (
            isinstance(uri, str)
            and "/commune/" in uri
            and not v[postal_codes_attribute]
            and uri in arrondissements_by_parent
        ):
            postal_codes = set()
            for arr_index in arrondissements_by_parent[uri]:
                postal_codes |= set(graph.vs[arr_index][postal_codes_attribute])
            if postal_codes:
                v[postal_codes_attribute] = sorted(postal_codes)


def get_postal_codes_historical_geo_entity(
    graph: igraph.Graph,
    hexasmal_data_dict: dict[str, list[tuple[str, str, str, str]]],
    insee_colname_uri_label: str,
    insee_colname_label_label: str,
    insee_colname_insee_code_label: str,
    postal_codes_attribute: str,
) -> None:
    """
    Retrodicate the postal codes of the current (leaf) entities to the
    historical entities of each connected component of the event graph.

    If every leaf of a component shares the same single postal code, it is
    propagated to the whole component. Otherwise, the Hexasmal labels
    (``delivery_label`` / ``associated_name``) of the component are matched
    against the INSEE labels, and the remaining vertices inherit postal codes
    from their predecessors or their unique successor.
    """
    components = graph.connected_components(mode="weak")
    for component in components:
        if len(component) < 2:
            continue
        subgraph = graph.subgraph(component)
        postal_codes_by_vertex: dict[int, set[str]] = {
            i: set(subgraph.vs[i][postal_codes_attribute])
            for i in range(subgraph.vcount())
        }
        leaves = [
            i for i in range(subgraph.vcount()) if subgraph.degree(i, mode="OUT") == 0
        ]
        if not leaves:
            continue
        leaves_postal_codes = [tuple(sorted(postal_codes_by_vertex[i])) for i in leaves]
        if (
            all(len(p) == 1 for p in leaves_postal_codes)
            and len(set(leaves_postal_codes)) == 1
        ):
            shared_postal_code = list(leaves_postal_codes[0])
            for i in range(subgraph.vcount()):
                if not postal_codes_by_vertex[i]:
                    postal_codes_by_vertex[i] = set(shared_postal_code)
        else:
            dedicated = assign_postal_codes_by_hexasmal_labels(
                subgraph=subgraph,
                hexasmal_data_dict=hexasmal_data_dict,
                postal_codes_by_vertex=postal_codes_by_vertex,
                insee_colname_label_label=insee_colname_label_label,
                insee_colname_insee_code_label=insee_colname_insee_code_label,
            )
            propagate_postal_codes_in_component(
                subgraph=subgraph,
                postal_codes_by_vertex=postal_codes_by_vertex,
                dedicated=dedicated,
            )
        for local_index, global_index in enumerate(component):
            graph.vs[global_index][postal_codes_attribute] = sorted(
                postal_codes_by_vertex[local_index]
            )


def merge_postal_codes(
    duckdb_connection: duckdb.DuckDBPyConnection,
    current_stored_data: InfoCurrentStoredData,
    output_dir: Path,
) -> None:
    insee_colname_uri_label = "uri"
    insee_colname_label_label = "label"
    insee_colname_end_date_label = "end_date"
    insee_colname_insee_code_label = "insee_code"

    logger.info(
        re.sub(
            string="""
            Add columns postal_codes and postal_codes_count
            to the communes and arrondissents_municipaux tables.
            """.replace("\r\n", "\n")
            .replace("\n", " ")
            .replace("\t", " "),
            pattern=r"[ ]+",
            repl=" ",
        )
    )
    communes_metadata = current_stored_data.get_data(key="insee_communes")
    communes_path = communes_metadata.path
    if isinstance(communes_metadata, GeoCSVFileMetadata):
        communes_colnames = communes_metadata.colnames
    else:
        raise TypeError(
            "The metadata for the communes table is not a GeoCSVFileMetadata"
        )

    arrondissements_municipaux_metadata = current_stored_data.get_data(
        key="insee_arrondissements_municipaux"
    )
    arrondissements_municipaux_path = arrondissements_municipaux_metadata.path
    if isinstance(arrondissements_municipaux_metadata, GeoCSVFileMetadata):
        arrondissements_municipaux_colnames = (
            arrondissements_municipaux_metadata.colnames
        )
    else:
        raise TypeError(
            "The metadata for the arrondissents_municipaux table is not a GeoCSVFileMetadata"
        )

    laposte_hexasmal_metadata = current_stored_data.get_data(key="laposte_hexasmal")
    if not isinstance(laposte_hexasmal_metadata, GeoCSVFileMetadata):
        raise TypeError(
            "The metadata for the laposte_hexasmal table is not a GeoCSVFileMetadata"
        )

    graph = build_history_graph_of_geo_entities(
        duckdb_connection=duckdb_connection,
        current_stored_data=current_stored_data,
        tables=["insee_communes", "insee_arrondissements_municipaux"],
    )

    table_name_laposte_hexasmal = "laposte_hexasmal"
    sql_query = f"""
    CREATE OR REPLACE VIEW {table_name_laposte_hexasmal} AS (
        {laposte_hexasmal_metadata.get_duckdb_sql_import_query()}
    );
    """

    hexasmal_data_dict: dict[str, list[tuple[str, str, str, str]]] = {}
    try:
        duckdb_connection.execute(sql_query)
    except Exception as e:
        raise RuntimeError("Failed to execute query on duckdb") from e

    try:
        duckdb_connection.execute(f"FROM {table_name_laposte_hexasmal} ;")
    except Exception as e:
        raise RuntimeError(
            f"Failed to execute query on view {table_name_laposte_hexasmal}"
        ) from e

    res_fetched = duckdb_connection.fetchone()
    while res_fetched is not None:
        if isinstance(res_fetched[0], str):
            if res_fetched[0] in hexasmal_data_dict:
                hexasmal_data_dict[res_fetched[0]].append(
                    (res_fetched[1], res_fetched[2], res_fetched[3], res_fetched[4])
                )
            else:
                hexasmal_data_dict[res_fetched[0]] = [
                    (res_fetched[1], res_fetched[2], res_fetched[3], res_fetched[4])
                ]
        res_fetched = duckdb_connection.fetchone()

    end_date_colname = None
    uri_colname = None
    insee_code_colname = None
    insee_label_colname = None
    for x in communes_colnames:
        if x.name == insee_colname_end_date_label:
            end_date_colname = x
        elif x.name == insee_colname_uri_label:
            uri_colname = x
        elif x.name == insee_colname_insee_code_label:
            insee_code_colname = x
        elif x.name == insee_colname_label_label:
            insee_label_colname = x
    if uri_colname is None:
        raise RuntimeError(
            f"The column '{insee_colname_uri_label}' is not found in the communes table."
        )
    if not isinstance(uri_colname, StringColumnDataType):
        raise TypeError(
            re.sub(
                string=f"""
                The column '{insee_colname_uri_label}'
                is not a StringColumnDataType in communes table
                """.replace("\r\n", "\n")
                .replace("\n", " ")
                .replace("\t", " "),
                pattern=r"[ ]+",
                repl=" ",
            )
        )
    if end_date_colname is None:
        raise RuntimeError(
            f"The column '{insee_colname_end_date_label}' is not found in the communes table."
        )
    if not isinstance(end_date_colname, DateColumnDataType):
        raise TypeError(
            re.sub(
                string=f"""
                The column '{insee_colname_end_date_label}'
                is not a DateColumnDataType in communes table
                """.replace("\r\n", "\n")
                .replace("\n", " ")
                .replace("\t", " "),
                pattern=r"[ ]+",
                repl=" ",
            )
        )
    if insee_code_colname is None:
        raise RuntimeError(
            re.sub(
                string=f"""
                The column '{insee_colname_insee_code_label}'
                is not found in the communes table.
                """.replace("\r\n", "\n")
                .replace("\n", " ")
                .replace("\t", " "),
                pattern=r"[ ]+",
                repl=" ",
            )
        )
    if not isinstance(insee_code_colname, StringColumnDataType):
        raise TypeError(
            re.sub(
                string=f"""
                The column '{insee_colname_insee_code_label}'
                is not a StringColumnDataType in communes table
                """.replace("\r\n", "\n")
                .replace("\n", " ")
                .replace("\t", " "),
                pattern=r"[ ]+",
                repl=" ",
            )
        )
    if insee_label_colname is None:
        raise RuntimeError(
            f"The column '{insee_colname_label_label}' is not found in the communes table."
        )
    if not isinstance(insee_label_colname, StringColumnDataType):
        raise TypeError(
            re.sub(
                string=f"""
                The column '{insee_colname_label_label}'
                is not a StringColumnDataType in communes table
                """.replace("\r\n", "\n")
                .replace("\n", " ")
                .replace("\t", " "),
                pattern=r"[ ]+",
                repl=" ",
            )
        )

    end_date_colname = None
    uri_colname = None
    insee_code_colname = None
    insee_label_colname = None
    for x in arrondissements_municipaux_colnames:
        if x.name == insee_colname_end_date_label:
            end_date_colname = x
        elif x.name == insee_colname_uri_label:
            uri_colname = x
        elif x.name == insee_colname_insee_code_label:
            insee_code_colname = x
        elif x.name == insee_colname_label_label:
            insee_label_colname = x
    if uri_colname is None:
        raise RuntimeError(
            re.sub(
                string=f"""
                The column '{insee_colname_uri_label}'
                is not found in the arrondissements_municipaux table.
                """.replace("\r\n", "\n")
                .replace("\n", " ")
                .replace("\t", " "),
                pattern=r"[ ]+",
                repl=" ",
            )
        )
    if not isinstance(uri_colname, StringColumnDataType):
        raise TypeError(
            re.sub(
                string=f"""
                The column '{insee_colname_uri_label}' is not
                a StringColumnDataType in arrondissements_municipaux table
                """.replace("\r\n", "\n")
                .replace("\n", " ")
                .replace("\t", " "),
                pattern=r"[ ]+",
                repl=" ",
            )
        )
    if end_date_colname is None:
        raise RuntimeError(
            re.sub(
                string=f"""
                The column '{insee_colname_end_date_label}'
                is not found in the arrondissements_municipaux table.
                """.replace("\r\n", "\n")
                .replace("\n", " ")
                .replace("\t", " "),
                pattern=r"[ ]+",
                repl=" ",
            )
        )
    if not isinstance(end_date_colname, DateColumnDataType):
        raise TypeError(
            re.sub(
                string=f"""
                The column '{insee_colname_end_date_label}'
                is not a DateColumnDataType in arrondissements_municipaux table
                """.replace("\r\n", "\n")
                .replace("\n", " ")
                .replace("\t", " "),
                pattern=r"[ ]+",
                repl=" ",
            )
        )
    if insee_code_colname is None:
        raise RuntimeError(
            re.sub(
                string=f"""
                The column '{insee_colname_insee_code_label}'
                is not found in the arrondissements_municipaux table.
                """.replace("\r\n", "\n")
                .replace("\n", " ")
                .replace("\t", " "),
                pattern=r"[ ]+",
                repl=" ",
            )
        )
    if not isinstance(insee_code_colname, StringColumnDataType):
        raise TypeError(
            re.sub(
                string=f"""
                The column '{insee_colname_insee_code_label}'
                is not a StringColumnDataType in arrondissements_municipaux table
                """.replace("\r\n", "\n")
                .replace("\n", " ")
                .replace("\t", " "),
                pattern=r"[ ]+",
                repl=" ",
            )
        )
    if insee_label_colname is None:
        raise RuntimeError(
            re.sub(
                string=f"""
                The column '{insee_colname_label_label}'
                is not found in the arrondissements_municipaux table.
                """.replace("\r\n", "\n")
                .replace("\n", " ")
                .replace("\t", " "),
                pattern=r"[ ]+",
                repl=" ",
            )
        )
    if not isinstance(insee_label_colname, StringColumnDataType):
        raise TypeError(
            re.sub(
                string=f"""
                The column '{insee_colname_label_label}' is not
                a StringColumnDataType in arrondissements_municipaux table
                """.replace("\r\n", "\n")
                .replace("\n", " ")
                .replace("\t", " "),
                pattern=r"[ ]+",
                repl=" ",
            )
        )

    graph_postal_codes_attributes = "postal_codes"
    graph.vs[graph_postal_codes_attributes] = [
        get_postal_codes_current_geo_entity(
            vertex=v,
            hexasmal_data_dict=hexasmal_data_dict,
            insee_colname_uri_label=insee_colname_uri_label,
            insee_colname_label_label=insee_colname_label_label,
            insee_colname_insee_code_label=insee_colname_insee_code_label,
            insee_colname_end_date_label=insee_colname_end_date_label,
        )
        for v in graph.vs
    ]
    current_count_assigned = len(
        [True for x in graph.vs[graph_postal_codes_attributes] if len(x) > 0]
    )
    previous_count_affected = 0

    print(f"current_count_assigned : {current_count_assigned}")
    print(f"previous_count_affected : {previous_count_affected}")

    while current_count_assigned > previous_count_affected:
        get_postal_codes_historical_geo_entity(
            graph=graph,
            hexasmal_data_dict=hexasmal_data_dict,
            insee_colname_uri_label=insee_colname_uri_label,
            insee_colname_label_label=insee_colname_label_label,
            insee_colname_insee_code_label=insee_colname_insee_code_label,
            postal_codes_attribute=graph_postal_codes_attributes,
        )
        previous_count_affected = current_count_assigned
        current_count_assigned = len(
            [True for x in graph.vs[graph_postal_codes_attributes] if len(x) > 0]
        )
        print(f"current_count_assigned : {current_count_assigned}")
        print(f"previous_count_affected : {previous_count_affected}")

    assign_postal_codes_from_arrondissements(
        graph=graph,
        insee_colname_uri_label=insee_colname_uri_label,
        insee_colname_parent_uri_label="parent_uri",
        postal_codes_attribute=graph_postal_codes_attributes,
    )

    new_col_postal_codes_communes = StringListColumnDataType(
        name="postal_codes", sep="|"
    )
    new_col_postal_codes_arrondissements_municipaux = StringListColumnDataType(
        name="postal_codes", sep="|"
    )
    new_col_postal_codes_count_communes = IntegerColumnDataType(
        name="postal_codes_count"
    )
    new_col_postal_codes_count_arrondissements_municipaux = IntegerColumnDataType(
        name="postal_codes_count"
    )
    new_col_cluster_id_communes = IntegerColumnDataType(name="cluster_id")
    new_col_cluster_id_arrondissements_municipaux = IntegerColumnDataType(
        name="cluster_id"
    )
    new_col_cluster_size_communes = IntegerColumnDataType(name="cluster_size")
    new_col_cluster_size_arrondissements_municipaux = IntegerColumnDataType(
        name="cluster_size"
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv_path_communes = output_dir / communes_path.name
    output_csv_path_arrondissements_municipaux = (
        output_dir / arrondissements_municipaux_path.name
    )

    components = graph.connected_components(mode="weak")
    components_sizes = components.sizes()

    with (
        open(
            output_csv_path_communes, mode="w", newline="", encoding="utf-8"
        ) as f_communes,
        open(
            output_csv_path_arrondissements_municipaux,
            mode="w",
            newline="",
            encoding="utf-8",
        ) as f_arrondissements_municipaux,
    ):
        writer_communes = csv.DictWriter(
            f=f_communes,
            fieldnames=[x.name for x in communes_colnames]
            + [
                new_col_postal_codes_communes.name,
                new_col_postal_codes_count_communes.name,
                new_col_cluster_id_communes.name,
                new_col_cluster_size_communes.name,
            ],
        )
        writer_communes.writeheader()
        writer_arrondissements_municipaux = csv.DictWriter(
            f=f_arrondissements_municipaux,
            fieldnames=[x.name for x in arrondissements_municipaux_colnames]
            + [
                new_col_postal_codes_arrondissements_municipaux.name,
                new_col_postal_codes_count_arrondissements_municipaux.name,
                new_col_cluster_id_arrondissements_municipaux.name,
                new_col_cluster_size_arrondissements_municipaux.name,
            ],
        )
        writer_arrondissements_municipaux.writeheader()
        for v in graph.vs:
            geo_entity = get_geo_entity_type(v, uri_attr=uri_colname.name)
            if geo_entity == "commune":
                row_output = {}
                for col in communes_colnames:
                    row_output[col.name] = v[col.name]
                row_output[new_col_postal_codes_communes.name] = (
                    new_col_postal_codes_communes.sep.join(
                        v[graph_postal_codes_attributes]
                    )
                )
                row_output[new_col_postal_codes_count_communes.name] = len(
                    v[graph_postal_codes_attributes]
                )
                row_output[new_col_cluster_id_communes.name] = components.membership[
                    v.index
                ]
                row_output[new_col_cluster_size_communes.name] = components_sizes[
                    components.membership[v.index]
                ]
                writer_communes.writerow(row_output)
            elif geo_entity == "arrondissementMunicipal":
                row_output = {}
                for col in arrondissements_municipaux_colnames:
                    row_output[col.name] = v[col.name]
                row_output[new_col_postal_codes_arrondissements_municipaux.name] = (
                    new_col_postal_codes_arrondissements_municipaux.sep.join(
                        v[graph_postal_codes_attributes]
                    )
                )
                row_output[
                    new_col_postal_codes_count_arrondissements_municipaux.name
                ] = len(v[graph_postal_codes_attributes])
                row_output[new_col_cluster_id_arrondissements_municipaux.name] = (
                    components.membership[v.index]
                )
                row_output[new_col_cluster_size_arrondissements_municipaux.name] = (
                    components_sizes[components.membership[v.index]]
                )
                writer_arrondissements_municipaux.writerow(row_output)
    current_stored_data.replace_data(
        key="insee_communes",
        value=GeoCSVFileMetadata(
            path=output_csv_path_communes,
            header=True,
            delim=",",
            encoding="utf-8",
            colnames=communes_colnames
            + [
                new_col_postal_codes_communes,
                new_col_postal_codes_count_communes,
                new_col_cluster_id_communes,
                new_col_cluster_size_communes,
            ],
        ),
    )
    current_stored_data.replace_data(
        key="insee_arrondissements_municipaux",
        value=GeoCSVFileMetadata(
            path=output_csv_path_arrondissements_municipaux,
            header=True,
            delim=",",
            encoding="utf-8",
            colnames=arrondissements_municipaux_colnames
            + [
                new_col_postal_codes_arrondissements_municipaux,
                new_col_postal_codes_count_arrondissements_municipaux,
                new_col_cluster_id_arrondissements_municipaux,
                new_col_cluster_size_arrondissements_municipaux,
            ],
        ),
    )
    logger.info(
        re.sub(
            string="""
            Added columns postal_codes and postal_codes_count
            to the communes and arrondissents_municipaux tables.
            """.replace("\r\n", "\n")
            .replace("\n", " ")
            .replace("\t", " "),
            pattern=r"[ ]+",
            repl=" ",
        )
    )
