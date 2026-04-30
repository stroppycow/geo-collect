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


def get_vertices_info(
        vertices: list[igraph.Vertex],
        insee_colname_uri_label: str,
        insee_colname_label_label: str,
        insee_colname_insee_code_label: str,
        postal_codes_attribute: str
    ) -> list[dict]:
    res = []
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
        if not isinstance(postal_codes, list):
            raise RuntimeError("The postal_codes of the vertex is not a list for {uri}".format(uri=uri))
        if postal_codes:
            if any([not isinstance(x, str) for x in postal_codes]):
                raise RuntimeError("The postal_codes of the vertex contains non string elements for {uri}".format(uri=uri))
        res.append(
            {
                insee_colname_uri_label: uri,
                insee_colname_label_label: insee_label,
                insee_colname_insee_code_label: insee_code,
                postal_codes_attribute: postal_codes
            }
        )
    return res
        

def get_postal_codes_historical_geo_entity_when_children_have_single_postal_code(
        vertex: igraph.Vertex,
        insee_colname_uri_label: str,
        insee_colname_label_label: str,
        insee_colname_insee_code_label: str,
        insee_colname_end_date_label: str,
        postal_codes_attribute: str
    ) -> list[str]:
    uri : Optional[str] = None
    end_date : Optional[datetime.date] = None
    postal_codes: list[str] = []
    try:
        uri = vertex[insee_colname_uri_label]                   
    except:
        pass
    try:
        end_date = vertex[insee_colname_end_date_label]                   
    except:
        pass
    try:
        postal_codes = vertex[postal_codes_attribute]                   
    except:
        pass
    if not isinstance(uri, str):
        raise RuntimeError("The uri of the vertex is not a string. It is {uri_type}".format(uri_type=type(uri)))
    if end_date is None:
        if not isinstance(postal_codes, list):
            raise RuntimeError("The postal_codes of the vertex is not a list for {uri}".format(uri=uri))
        if postal_codes:
            if any([not isinstance(x, str) for x in postal_codes]):
                raise RuntimeError("The postal_codes of the vertex contains non string elements for {uri}".format(uri=uri))
        return postal_codes
    else:
        leaves_geo_entity = get_leaves_geo_entity_graph(vertex)
        leaves_info = get_vertices_info(
            vertices=leaves_geo_entity,
            insee_colname_uri_label=insee_colname_uri_label,
            insee_colname_label_label=insee_colname_label_label,
            insee_colname_insee_code_label=insee_colname_insee_code_label,
            postal_codes_attribute=postal_codes_attribute
        )
        postal_codes_leaves = list(set(chain.from_iterable([x[postal_codes_attribute] for x in leaves_info])))
        if postal_codes_leaves:
            if len(postal_codes_leaves) == 1:
                return postal_codes_leaves
            else:
                return []
        else:
            return []
        
def get_postal_codes_historical_geo_entity_when_path_to_leaf_is_unique_in_and_out(
        vertex: igraph.Vertex,
        insee_colname_uri_label: str,
        insee_colname_label_label: str,
        insee_colname_insee_code_label: str,
        postal_codes_attribute: str
    ) -> list[str]:
    uri : Optional[str] = None
    postal_codes: list[str] = []
    try:
        uri = vertex[insee_colname_uri_label]                   
    except:
        pass
    try:
        postal_codes = vertex[postal_codes_attribute]                   
    except:
        pass
    if not isinstance(uri, str):
        raise RuntimeError("The uri of the vertex is not a string. It is {uri_type}".format(uri_type=type(uri)))
    if not isinstance(postal_codes, list):
        raise RuntimeError("The postal_codes of the vertex is not a list for {uri}".format(uri=uri))
    if postal_codes:
        if any([not isinstance(x, str) for x in postal_codes]):
            raise RuntimeError("The postal_codes of the vertex contains non string elements for {uri}".format(uri=uri))
        return postal_codes
    else:
        if path_to_leaf_is_unique_in_and_out(vertex=vertex):
            leaves_geo_entity = get_leaves_geo_entity_graph(vertex)
            leaves_info = get_vertices_info(
                vertices=leaves_geo_entity,
                insee_colname_uri_label=insee_colname_uri_label,
                insee_colname_label_label=insee_colname_label_label,
                insee_colname_insee_code_label=insee_colname_insee_code_label,
                postal_codes_attribute=postal_codes_attribute
            )
            return list(set(chain.from_iterable([x[postal_codes_attribute] for x in leaves_info])))
        else:
            return []
        
def get_postal_codes_historical_geo_entity(
        vertex: igraph.Vertex,
        insee_colname_uri_label: str,
        insee_colname_label_label: str,
        insee_colname_insee_code_label: str,
        postal_codes_attribute: str
    ) -> list[str]:
    uri : Optional[str] = None
    label: Optional[str] = None
    postal_codes: list[str] = []
    try:
        uri = vertex[insee_colname_uri_label]                   
    except:
        pass
    try:
        postal_codes = vertex[postal_codes_attribute]                   
    except:
        pass
    try:
        label = vertex[insee_colname_label_label]                   
    except:
        pass
    if not isinstance(uri, str):
        raise RuntimeError("The uri of the vertex is not a string. It is {uri_type}".format(uri_type=type(uri)))
    if not isinstance(uri, str):
        raise RuntimeError("The uri of the vertex is not a string. It is {uri_type}".format(uri_type=type(uri)))
    if not isinstance(postal_codes, list):
        raise RuntimeError("The postal_codes of the vertex is not a list for {uri}".format(uri=uri))
    if postal_codes:
        if any([not isinstance(x, str) for x in postal_codes]):
            raise RuntimeError("The postal_codes of the vertex contains non string elements for {uri}".format(uri=uri))
        return postal_codes
    else:
        predecessors_geo_entities = [vertex.graph.vs[i] for i in vertex.graph.neighbors(vertex.index, mode="IN")]
        predecessors_info = get_vertices_info(
            vertices=predecessors_geo_entities,
            insee_colname_uri_label=insee_colname_uri_label,
            insee_colname_label_label=insee_colname_label_label,
            insee_colname_insee_code_label=insee_colname_insee_code_label,
            postal_codes_attribute=postal_codes_attribute
        )
        # We concatenate all the ZIP codes of the immediate preceding entities
        # if those entities all have a ZIP code and if the only entity following them is this one
        if any([not(len(d[postal_codes_attribute]) > 0 and v.degree(mode="OUT") ==  1) for d,v in zip(predecessors_info, predecessors_geo_entities)]):
            return list(set(chain.from_iterable([x[postal_codes_attribute] for x in predecessors_info])))
    

        successors_geo_entities = [vertex.graph.vs[i] for i in vertex.graph.neighbors(vertex.index, mode="OUT")]
        successors_info = get_vertices_info(
            vertices=successors_geo_entities,
            insee_colname_uri_label=insee_colname_uri_label,
            insee_colname_label_label=insee_colname_label_label,
            insee_colname_insee_code_label=insee_colname_insee_code_label,
            postal_codes_attribute=postal_codes_attribute
        )
        #The search for a ZIP code for this entity continues only if all subsequent entities have been assigned a ZIP code
        if len([d[postal_codes_attribute] for d in successors_info if len(d[postal_codes_attribute]) == 0]) > 0:
            return []

        successors_postal_codes = list(set(chain.from_iterable([x[postal_codes_attribute] for x in successors_info])))
        if len(successors_postal_codes) == 1:
            return successors_postal_codes
        elif len(successors_geo_entities) == 1 and successors_geo_entities[0].degree(mode="IN") == 1:
            return successors_postal_codes
        return []


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
    previous_count_affected = current_count_assigned + 1
    
    while current_count_assigned < previous_count_affected:
        graph.vs[graph_postal_codes_attributes] = [
            get_postal_codes_historical_geo_entity(
                vertex=v,
                insee_colname_uri_label=insee_colname_uri_label,
                insee_colname_label_label=insee_colname_label_label,
                insee_colname_insee_code_label=insee_colname_insee_code_label,
                postal_codes_attribute=graph_postal_codes_attributes
            ) for v in graph.vs
        ]
        previous_count_affected = current_count_assigned
        current_count_assigned = len([True for x in graph.vs[graph_postal_codes_attributes] if len(x) > 0 ]) 

    new_col_postal_codes_communes = StringListColumnDataType(name='postal_codes', sep='|')
    new_col_postal_codes_arrondissements_municipaux =  StringListColumnDataType(name='postal_codes', sep='|')
    new_col_postal_codes_count_communes = IntegerColumnDataType(name='postal_codes_count')
    new_col_postal_codes_count_arrondissements_municipaux =  IntegerColumnDataType(name='postal_codes_count')

    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv_path_communes = output_dir / communes_path.name
    output_csv_path_arrondissements_municipaux = output_dir / arrondissements_municipaux_path.name
    with open(output_csv_path_communes, mode="w", newline="", encoding="utf-8") as f_communes, open(output_csv_path_arrondissements_municipaux, mode="w", newline="", encoding="utf-8") as f_arrondissements_municipaux:
        writer_communes = csv.DictWriter(
            f=f_communes,
            fieldnames=[x.name for x in communes_colnames]+[new_col_postal_codes_communes.name, new_col_postal_codes_count_communes.name]
        )
        writer_communes.writeheader()
        writer_arrondissements_municipaux= csv.DictWriter(
            f=f_arrondissements_municipaux,
            fieldnames=[x.name for x in arrondissements_municipaux_colnames]+[new_col_postal_codes_arrondissements_municipaux.name, new_col_postal_codes_count_arrondissements_municipaux.name]
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
                writer_arrondissements_municipaux.writerow(row_output)
    current_stored_data.replace_data(
        key='insee_communes',
        value=GeoCSVFileMetadata(
            path=output_csv_path_communes,
            header=True,
            delim=',',
            encoding='utf-8',
            colnames=communes_colnames+[new_col_postal_codes_communes, new_col_postal_codes_count_communes]
        )
    )
    current_stored_data.replace_data(
        key='insee_arrondissements_municipaux',
        value=GeoCSVFileMetadata(
            path=output_csv_path_arrondissements_municipaux,
            header=True,
            delim=',',
            encoding='utf-8',
            colnames=arrondissements_municipaux_colnames+[new_col_postal_codes_arrondissements_municipaux, new_col_postal_codes_count_arrondissements_municipaux]
        )
    )
    logging.info("Added columns postal_codes and postal_codes_count to the communes and arrondissents_municipaux tables.")   
