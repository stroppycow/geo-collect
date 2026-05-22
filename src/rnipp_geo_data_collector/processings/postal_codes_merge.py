import duckdb
from pathlib import Path
import csv
import igraph
import logging
from typing import Optional
import datetime
import unicodedata
import re
from rapidfuzz import fuzz
from itertools import chain

from ..metadata import InfoCurrentStoredData, GeoCSVFileMetadata, DateColumnDataType, StringColumnDataType, StringListColumnDataType, IntegerColumnDataType
from ..graph.graph_event import build_history_graph_of_geo_entities


def get_geo_entity_type(vertex, uri_attr: str = 'uri') -> str:
    uri = None
    try:
        uri = vertex[uri_attr]
    except:
        pass
    if not(isinstance(uri, str)):
         raise RuntimeError("The uri is not a string")
    return re.sub(
        pattern = r"[/].+",
        repl = "", 
        string=uri.replace('http://id.insee.fr/geo/', '')
    )       

def normalize_location_label(s: str, first_word_only: bool = False) -> str:
    STOPWORDS = {'territoire', 'departement', 'l', 'le', 'la', 'de', 'd', 'a', 'les', 'en', 'au', 'aux', 'une', 'sur', 'du'}
    REPLACE_WORDS = {
        'st': 'saint',
        'ste': 'sainte',
        'pt': 'pont'
    }
    s = s.replace('œ', 'oe')
    s = s.replace('Œ', 'oe')
    s = s.lower()
    s = unicodedata.normalize('NFD', s)
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    s = s.replace("-", " ")
    s = s.replace("'", " ")
    s = s.replace("&", " ")
    s = s.replace(";", " ")
    s = re.sub(r'[^a-z0-9 ]', '', s)
    s = re.sub(r'([0-9]+)(e|eme|er|ier) ', r'\1 ', s)
    words = s.split()
    words = [w for w in words if w not in STOPWORDS]
    words = [w if w not in REPLACE_WORDS else REPLACE_WORDS[w] for w in words]
    if len(words) > 1:
        if words[len(words)-1] in ['arrondissement', 'arrond', 'arro']:
            words.pop()

    if first_word_only:
        try:
            return words[0]
        except:
            return ''

    return " ".join(words)

def is_label_location_similar(s1: str, s2: str, first_word_only : bool = False) -> bool:
    try:
        score = fuzz.ratio(normalize_location_label(s1, first_word_only=first_word_only), normalize_location_label(s2, first_word_only=first_word_only))
        if score >= 90:
            return True
    except:
        pass
    return False

def get_postal_codes_current_geo_entity(
    vertex: igraph.Vertex,
    hexasmal_data_dict: dict[str, list[tuple[str, str, str, str]]],
    insee_colname_uri_label: str,
    insee_colname_label_label: str,
    insee_colname_insee_code_label: str,
    insee_colname_end_date_label: str
) -> list[str]:
    uri : Optional[str] = None
    insee_label : Optional[str] = None
    insee_code : Optional[str] = None
    end_date : Optional[datetime.date] = None
    try:
        uri = vertex[insee_colname_uri_label]                   
    except:
        pass
    try:
        insee_label = vertex[insee_colname_label_label]                   
    except:
        pass
    try:
        insee_code = vertex[insee_colname_insee_code_label]                   
    except:
        pass
    try:
        end_date = vertex[insee_colname_end_date_label]                   
    except:
        pass
    if not isinstance(uri, str):
        raise RuntimeError("The uri of the vertex is not a string. It is {uri_type}".format(uri_type=type(uri)))
    if isinstance(insee_code, str) and isinstance(insee_label, str) and end_date is None:
        if insee_code in hexasmal_data_dict:
            laposte_labels : list[str] = []
            try:
                laposte_labels.extend([x[0] for x in hexasmal_data_dict[insee_code]])
            except:
                pass
            try:
                laposte_labels.extend([x[2] for x in hexasmal_data_dict[insee_code]])
            except:
                pass
            laposte_labels = list(set([x for x in laposte_labels if isinstance(x, str)]))
            if len(laposte_labels) > 0:
                if re.search(pattern='[(]Canton d', string=insee_label) is not None:
                    for l in laposte_labels:
                        if is_label_location_similar(l, insee_label, first_word_only = True):
                            return list(set([x[1] for x in hexasmal_data_dict[insee_code] if isinstance(x[1], str)]))
                else:
                    for l in laposte_labels:
                        if is_label_location_similar(l, insee_label, first_word_only = False):
                            return list(set([x[1] for x in hexasmal_data_dict[insee_code] if isinstance(x[1], str)]))
    return []


def get_leaves_geo_entity_graph(vertex: igraph.Vertex) -> list[igraph.Vertex]:
    reachable = vertex.graph.subcomponent(vertex.index, mode="OUT")
    leaves = [vertex.graph.vs[v] for v in reachable if vertex.graph.vs[v].degree(mode="OUT") == 0]
    return leaves

def path_to_leaf_is_unique_in_and_out(vertex: igraph.Vertex) -> bool:
    reachable = vertex.graph.subcomponent(vertex.index, mode="OUT")
    if [v for v in reachable if v != vertex.index and vertex.graph.vs[v].degree(mode="IN") > 1 ]:
        return False
    return True

class GeoEntityVertexInfo:
    def __init__(self, uri: str, label: str, insee_code: str, postal_codes: list[str]):
        self.uri = uri
        self.label = label
        self.insee_code = insee_code
        self.postal_codes = postal_codes


def get_vertices_info(
        graph: igraph.Graph,
        ids:list[int],
        insee_colname_uri_label: str,
        insee_colname_label_label: str,
        insee_colname_insee_code_label: str,
        postal_codes_attribute: str
    ) -> list[GeoEntityVertexInfo]:
    vertices: list = []
    for id in ids:
        try:
            vertices.append(graph.vs[id])
        except:
            pass
    res: list[GeoEntityVertexInfo] = []
    for v in vertices:   
        uri : Optional[str] = None
        insee_label : Optional[str] = None
        insee_code : Optional[str] = None
        postal_codes: list[str] = []
        try:
            uri = v[insee_colname_uri_label]                   
        except:
            pass
        try:
            insee_label = v[insee_colname_label_label]                   
        except:
            pass
        try:
            insee_code = v[insee_colname_insee_code_label]                   
        except:
            pass
        try:
            postal_codes = v[postal_codes_attribute]                   
        except:
            pass
        if not isinstance(uri, str):
            raise RuntimeError("The uri of the vertex is not a string. It is {uri_type}".format(uri_type=type(uri)))
        if not isinstance(insee_label, str):
            raise RuntimeError("The insee_label of the vertex is not a string for {uri}".format(uri=uri))
        if not isinstance(insee_code, str):
            raise RuntimeError("The insee_code of the vertex is not a string for {uri}".format(uri=uri))
        if isinstance(postal_codes, list) and all(isinstance(value, str) for value in postal_codes):
           res.append(
                GeoEntityVertexInfo(
                    uri=uri,
                    label=insee_label,
                    insee_code=insee_code,
                    postal_codes=postal_codes
                )
            )
        else:
            raise RuntimeError("The postal_codes of the vertex is not a list for {uri}".format(uri=uri))
    return res
        
        
def extract_hexasmal_labels_from_hexasmal_infos(
    hexasmal_data_dict: dict[str, list[tuple[str, str, str, str]]],
    info_nodes_out: list[GeoEntityVertexInfo]
) -> list[tuple[str, list[str]]]:
    hexasmal_labels: list[tuple[str, list[str]]] = []
    for n in info_nodes_out:
        hexasmal_infos: list[tuple[str, str, str, str]] = []
        try:
            hexasmal_infos = hexasmal_data_dict[n.insee_code]
        except:
            pass
        if len(hexasmal_infos) == 0:
            return []
        elif len(hexasmal_infos) == 1:
            if hexasmal_infos[0][2] is None:
                return []
            if hexasmal_infos[0][2] == '':
                return []
            hexasmal_labels.append((hexasmal_infos[0][2], [hexasmal_infos[0][1]]))
        else:
            hexasmal_postal_codes = [x[1] for x in hexasmal_infos if x[1] is not None and x[1] != '']
            hexasmal_delivery_label = [x[2] for x in hexasmal_infos if x[2] is not None and x[2] != '']
            hexasmal_associated_name = [x[3] for x in hexasmal_infos if x[3] is not None and x[3] != '']
            if len(hexasmal_associated_name) == 0 and len(hexasmal_delivery_label) == len(hexasmal_infos):
                if len(list(set(hexasmal_delivery_label))) == 1:
                    hexasmal_labels.append(
                        (
                            hexasmal_delivery_label[0],
                            list(set(hexasmal_postal_codes))
                        )
                    )
                else:
                    return []
            elif len(hexasmal_associated_name) == (len(hexasmal_infos) - 1) and len(hexasmal_delivery_label) == len(hexasmal_infos):
                if len(list(set(hexasmal_associated_name))) == (len(hexasmal_infos) - 1) and len(list(set(hexasmal_delivery_label)))==1:
                    hexasmal_labels.append(
                        (
                            hexasmal_delivery_label[0],
                            [x[1] for x in hexasmal_infos if not(x[3] is not None and x[3] != '')]
                        )
                    )
                    hexasmal_labels.extend(
                        [(x[3],[x[1]]) for x in hexasmal_infos if x[3] is not None and x[3] != '']
                    )
                else:
                    return []
            elif len(hexasmal_associated_name) == len(hexasmal_infos) and len(hexasmal_delivery_label) == len(hexasmal_infos):
                if len(list(set(hexasmal_associated_name))) == len(hexasmal_infos) and len(list(set(hexasmal_delivery_label))) == 1:
                    hexasmal_labels.extend(
                        [(x[3],[x[1]]) for x in hexasmal_infos if x[3] is not None and x[3] != '']
                    )
                elif len(list(set(hexasmal_associated_name))) == 1 and len(list(set(hexasmal_delivery_label))) == len(hexasmal_infos):
                    hexasmal_labels.append(
                        (
                            hexasmal_associated_name[0],
                            list(set(hexasmal_postal_codes))
                        )
                    )
                else:
                    return []
            else:
                return []
    if len(list(set([x[0] for x in hexasmal_labels]))) == len(hexasmal_labels):
        return hexasmal_labels
    else:
        return []


def match_hexasmal_labels_to_insee_labels(
        hexasmal_labels: list[tuple[str, list[str]]],
        info_nodes_in: list[GeoEntityVertexInfo]
    ) -> dict[str, list[str]]:
    output : dict[str, list[str]] = {}
    if len(hexasmal_labels) > 0 and len(info_nodes_in) == len(hexasmal_labels):
        similar_matrix = [[False]*len(info_nodes_in)]*len(hexasmal_labels)
        similar_matrix_transpose = [[False]*len(hexasmal_labels)]*len(info_nodes_in)
        for i,el in enumerate(hexasmal_labels):
            for j,info in enumerate(info_nodes_in):
                similar_matrix[i][j] = is_label_location_similar(s1=el[0], s2=info.label)
                similar_matrix_transpose[j][i] = similar_matrix[i][j]
        for i in range(len(hexasmal_labels)):
            filter_true = [j for j, x in enumerate(similar_matrix[i]) if x]
            if filter_true:
                if any(similar_matrix_transpose[i]):
                    output[info_nodes_in[filter_true[0]].uri] = hexasmal_labels[i][1]
                else:
                    return {}
            else:
                return {}
    return output


def get_postal_codes_historical_geo_entity(
        graph: igraph.Graph,
        hexasmal_data_dict: dict[str, list[tuple[str, str, str, str]]],
        insee_colname_uri_label: str,
        insee_colname_label_label: str,
        insee_colname_insee_code_label: str,
        postal_codes_attribute: str
    ) -> None:
    components  = graph.connected_components(mode="weak")
    for c in components:
        if len(c) > 1:
            subgraph = graph.subgraph(c)
            degrees_out = subgraph.degree(mode="OUT")
            degrees_in = subgraph.degree(mode="IN")
            nodes_out = [i for i, d in enumerate(degrees_out) if d == 0]
            nodes_in = [i for i, d in enumerate(degrees_in) if d == 0]
            info_nodes_out = get_vertices_info(
                graph=subgraph,
                ids=nodes_out,
                insee_colname_uri_label=insee_colname_uri_label,
                insee_colname_label_label=insee_colname_label_label,
                insee_colname_insee_code_label=insee_colname_insee_code_label,
                postal_codes_attribute=postal_codes_attribute
            )
            postal_codes_out =  list(set([y for x in info_nodes_out for y in x.postal_codes]))
            postal_codes_everywhere = all([len( x.postal_codes) > 0  for x in info_nodes_out])
            if len(nodes_out) > 0 and len(postal_codes_out) == 1 and len(postal_codes_out) == 1 and postal_codes_everywhere:
                for v in subgraph.vs:
                    graph.vs.find(name=v[insee_colname_uri_label])[postal_codes_attribute]=postal_codes_out
            else:
                """
                info_nodes_in = get_vertices_info(
                    graph=subgraph,
                    ids=nodes_in,
                    insee_colname_uri_label=insee_colname_uri_label,
                    insee_colname_label_label=insee_colname_label_label,
                    insee_colname_insee_code_label=insee_colname_insee_code_label,
                    postal_codes_attribute=postal_codes_attribute
                )
                hexasmal_labels = extract_hexasmal_labels_from_hexasmal_infos(
                    hexasmal_data_dict=hexasmal_data_dict,
                    info_nodes_out=info_nodes_out
                )
                output = match_hexasmal_labels_to_insee_labels(
                    hexasmal_labels=hexasmal_labels,
                    info_nodes_in=info_nodes_in
                )
                for k,v in output.items():
                    graph.vs.find(name=k)[postal_codes_attribute]=v
                """

            



def merge_postal_codes(
        duckdb_connection: duckdb.DuckDBPyConnection,
        current_stored_data: InfoCurrentStoredData,
        output_dir : Path
    ) -> None:
    insee_colname_uri_label = 'uri'
    insee_colname_label_label = 'label'
    insee_colname_end_date_label = 'end_date'
    insee_colname_insee_code_label = 'insee_code'

    logging.info("Add columns postal_codes and postal_codes_count to the communes and arrondissents_municipaux tables.")   
    communes_metadata=current_stored_data.get_data(key='insee_communes')
    communes_path = communes_metadata.path
    if isinstance(communes_metadata, GeoCSVFileMetadata):
        communes_colnames = communes_metadata.colnames
    else:
        raise RuntimeError("The metadata for the communes table is not a GeoCSVFileMetadata")
    
    arrondissements_municipaux_metadata = current_stored_data.get_data(key='insee_arrondissements_municipaux')
    arrondissements_municipaux_path = arrondissements_municipaux_metadata.path
    if isinstance(arrondissements_municipaux_metadata, GeoCSVFileMetadata):
        arrondissements_municipaux_colnames = arrondissements_municipaux_metadata.colnames
    else:
        raise RuntimeError("The metadata for the arrondissents_municipaux table is not a GeoCSVFileMetadata")
    
    laposte_hexasmal_metadata=current_stored_data.get_data(key='laposte_hexasmal')
    if isinstance(laposte_hexasmal_metadata, GeoCSVFileMetadata):
        laposte_hexasmal_colnames = laposte_hexasmal_metadata.colnames
    else:
        raise RuntimeError("The metadata for the laposte_hexasmal table is not a GeoCSVFileMetadata")
    
    graph = build_history_graph_of_geo_entities(
        duckdb_connection=duckdb_connection,
        current_stored_data=current_stored_data,
        tables=['insee_communes', 'insee_arrondissements_municipaux']
    )

    table_name_laposte_hexasmal = 'laposte_hexasmal'
    sql_query = """
    CREATE OR REPLACE VIEW {table_name} AS (
        {query}
    );
    """.format(
        table_name=table_name_laposte_hexasmal,
        query=laposte_hexasmal_metadata.get_duckdb_sql_import_query()
    )
    
    hexasmal_data_dict : dict[str, list[tuple[str, str, str, str]]] = {}
    try:
        duckdb_connection.execute(sql_query)
    except Exception as e:
        raise RuntimeError(f"Failed to execute query on duckdb") from e
    
    try:
        duckdb_connection.execute("FROM {table_name} ;".format(table_name=table_name_laposte_hexasmal))
    except Exception as e:
        raise RuntimeError(f"Failed to execute query on view {table_name_laposte_hexasmal}") from e
    
    res_fetched = duckdb_connection.fetchone()
    while res_fetched is not None:
        if isinstance(res_fetched[0], str):
            if res_fetched[0] in hexasmal_data_dict.keys():
                hexasmal_data_dict[res_fetched[0]].append((res_fetched[1],res_fetched[2], res_fetched[3], res_fetched[4]))
            else:
                hexasmal_data_dict[res_fetched[0]] = [(res_fetched[1],res_fetched[2], res_fetched[3], res_fetched[4])]
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
        raise RuntimeError(f"The column '{insee_colname_uri_label}' is not found in the communes table.")
    if not isinstance(uri_colname, StringColumnDataType):
        raise RuntimeError(f"The column '{insee_colname_uri_label}' is not a StringColumnDataType in communes table")
    if end_date_colname is None:
        raise RuntimeError(f"The column '{insee_colname_end_date_label}' is not found in the communes table.")
    if not isinstance(end_date_colname, DateColumnDataType):
        raise RuntimeError(f"The column '{insee_colname_end_date_label}' is not a DateColumnDataType in communes table")
    if insee_code_colname is None:
        raise RuntimeError(f"The column '{insee_colname_insee_code_label}' is not found in the communes table.")
    if not isinstance(insee_code_colname, StringColumnDataType):
        raise RuntimeError(f"The column '{insee_colname_insee_code_label}' is not a StringColumnDataType in communes table")
    if insee_label_colname is None:
        raise RuntimeError(f"The column '{insee_colname_label_label}' is not found in the communes table.")
    if not isinstance(insee_label_colname, StringColumnDataType):
        raise RuntimeError(f"The column '{insee_colname_label_label}' is not a StringColumnDataType in communes table")
    
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
        raise RuntimeError(f"The column '{insee_colname_uri_label}' is not found in the arrondissements_municipaux table.")
    if not isinstance(uri_colname, StringColumnDataType):
        raise RuntimeError(f"The column '{insee_colname_uri_label}' is not a StringColumnDataType in arrondissements_municipaux table")
    if end_date_colname is None:
        raise RuntimeError(f"The column '{insee_colname_end_date_label}' is not found in the arrondissements_municipaux table.")
    if not isinstance(end_date_colname, DateColumnDataType):
        raise RuntimeError(f"The column '{insee_colname_end_date_label}' is not a DateColumnDataType in arrondissements_municipaux table")
    if insee_code_colname is None:
        raise RuntimeError(f"The column '{insee_colname_insee_code_label}' is not found in the arrondissements_municipaux table.")
    if not isinstance(insee_code_colname, StringColumnDataType):
        raise RuntimeError(f"The column '{insee_colname_insee_code_label}' is not a StringColumnDataType in arrondissements_municipaux table")
    if insee_label_colname is None:
        raise RuntimeError(f"The column '{insee_colname_label_label}' is not found in the arrondissements_municipaux table.")
    if not isinstance(insee_label_colname, StringColumnDataType):
        raise RuntimeError(f"The column '{insee_colname_label_label}' is not a StringColumnDataType in arrondissements_municipaux table")

    graph_postal_codes_attributes = "postal_codes"
    graph.vs[graph_postal_codes_attributes] = [
        get_postal_codes_current_geo_entity(
            vertex=v,
            hexasmal_data_dict=hexasmal_data_dict,
            insee_colname_uri_label=insee_colname_uri_label,
            insee_colname_label_label=insee_colname_label_label,
            insee_colname_insee_code_label=insee_colname_insee_code_label,
            insee_colname_end_date_label=insee_colname_end_date_label
        ) for v in graph.vs
    ]
    current_count_assigned = len([True for x in graph.vs[graph_postal_codes_attributes] if len(x) > 0 ]) 
    previous_count_affected = 0

    print("current_count_assigned : {current_count_assigned}".format(current_count_assigned=current_count_assigned))
    print("previous_count_affected : {previous_count_affected}".format(previous_count_affected=previous_count_affected))

    while current_count_assigned > previous_count_affected:
        get_postal_codes_historical_geo_entity(
            graph=graph,
            hexasmal_data_dict=hexasmal_data_dict,
            insee_colname_uri_label=insee_colname_uri_label,
            insee_colname_label_label=insee_colname_label_label,
            insee_colname_insee_code_label=insee_colname_insee_code_label,
            postal_codes_attribute=graph_postal_codes_attributes
        )
        previous_count_affected = current_count_assigned
        current_count_assigned = len([True for x in graph.vs[graph_postal_codes_attributes] if len(x) > 0 ]) 
        print("current_count_assigned : {current_count_assigned}".format(current_count_assigned=current_count_assigned))
        print("previous_count_affected : {previous_count_affected}".format(previous_count_affected=previous_count_affected))

    new_col_postal_codes_communes = StringListColumnDataType(name='postal_codes', sep='|')
    new_col_postal_codes_arrondissements_municipaux =  StringListColumnDataType(name='postal_codes', sep='|')
    new_col_postal_codes_count_communes = IntegerColumnDataType(name='postal_codes_count')
    new_col_postal_codes_count_arrondissements_municipaux =  IntegerColumnDataType(name='postal_codes_count')
    new_col_cluster_id_communes = IntegerColumnDataType(name='cluster_id')
    new_col_cluster_id_arrondissements_municipaux = IntegerColumnDataType(name='cluster_id')
    new_col_cluster_size_communes = IntegerColumnDataType(name='cluster_size')
    new_col_cluster_size_arrondissements_municipaux = IntegerColumnDataType(name='cluster_size')
  
    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv_path_communes = output_dir / communes_path.name
    output_csv_path_arrondissements_municipaux = output_dir / arrondissements_municipaux_path.name
    
    components = graph.connected_components(mode="weak")
    components_sizes = components.sizes()

    with open(output_csv_path_communes, mode="w", newline="", encoding="utf-8") as f_communes, open(output_csv_path_arrondissements_municipaux, mode="w", newline="", encoding="utf-8") as f_arrondissements_municipaux:
        writer_communes = csv.DictWriter(
            f=f_communes,
            fieldnames=[x.name for x in communes_colnames]+[
                new_col_postal_codes_communes.name,
                new_col_postal_codes_count_communes.name,
                new_col_cluster_id_communes.name,
                new_col_cluster_size_communes.name
            ]
        )
        writer_communes.writeheader()
        writer_arrondissements_municipaux= csv.DictWriter(
            f=f_arrondissements_municipaux,
            fieldnames=[x.name for x in arrondissements_municipaux_colnames]+[
                new_col_postal_codes_arrondissements_municipaux.name,
                new_col_postal_codes_count_arrondissements_municipaux.name,
                new_col_cluster_id_arrondissements_municipaux.name,
                new_col_cluster_size_arrondissements_municipaux.name
            ]
        )
        writer_arrondissements_municipaux.writeheader()
        for v in graph.vs:
            geo_entity = get_geo_entity_type(v, uri_attr=uri_colname.name)
            if geo_entity == 'commune':
                row_output = {}
                for col in communes_colnames:
                    try:
                        row_output[col.name] = v[col.name]
                    except KeyError as e:
                        raise e
                row_output[new_col_postal_codes_communes.name] = new_col_postal_codes_communes.sep.join(v[graph_postal_codes_attributes])
                row_output[new_col_postal_codes_count_communes.name] = len(v[graph_postal_codes_attributes])
                row_output[new_col_cluster_id_communes.name] = components.membership[v.index]
                row_output[new_col_cluster_size_communes.name] = components_sizes[components.membership[v.index]]
                writer_communes.writerow(row_output)
            elif geo_entity == 'arrondissementMunicipal':
                row_output = {}
                for col in arrondissements_municipaux_colnames:
                    try:
                        row_output[col.name] = v[col.name]
                    except KeyError as e:
                        raise e
                row_output[new_col_postal_codes_arrondissements_municipaux.name] = new_col_postal_codes_arrondissements_municipaux.sep.join(v[graph_postal_codes_attributes])
                row_output[new_col_postal_codes_count_arrondissements_municipaux.name] = len(v[graph_postal_codes_attributes])
                row_output[new_col_cluster_id_arrondissements_municipaux.name] = components.membership[v.index]
                row_output[new_col_cluster_size_arrondissements_municipaux.name] = components_sizes[components.membership[v.index]]
                writer_arrondissements_municipaux.writerow(row_output)
    current_stored_data.replace_data(
        key='insee_communes',
        value=GeoCSVFileMetadata(
            path=output_csv_path_communes,
            header=True,
            delim=',',
            encoding='utf-8',
            colnames=communes_colnames+[
                new_col_postal_codes_communes,
                new_col_postal_codes_count_communes,
                new_col_cluster_id_communes,
                new_col_cluster_size_communes
            ]
        )
    )
    current_stored_data.replace_data(
        key='insee_arrondissements_municipaux',
        value=GeoCSVFileMetadata(
            path=output_csv_path_arrondissements_municipaux,
            header=True,
            delim=',',
            encoding='utf-8',
            colnames=arrondissements_municipaux_colnames+[
                new_col_postal_codes_arrondissements_municipaux,
                new_col_postal_codes_count_arrondissements_municipaux, 
                new_col_cluster_id_arrondissements_municipaux,
                new_col_cluster_size_arrondissements_municipaux
            ]
        )
    )
    logging.info("Added columns postal_codes and postal_codes_count to the communes and arrondissents_municipaux tables.")   
