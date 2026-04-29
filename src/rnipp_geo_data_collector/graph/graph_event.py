import igraph
import duckdb
import re
from pathlib import Path
import pystache
from typing import Optional

from ..metadata import InfoCurrentStoredData, GeoCSVFileMetadata, ColumnDataType

def find_column_index_in_duckdb_output(label: str, desc: list[tuple]) -> int:
    output = []
    for i,d in enumerate(desc):
        try:
            if d[0] == label:
                output.append(i)
        except Exception as e:
            raise RuntimeError("Invalid description of a duckdb table")
    if len(output) == 0:
        raise RuntimeError(f"Column {label} not found")
    elif len(output) > 1:
        raise RuntimeError(f"Column {label} is not unique in the duckdb table")
    else:
        return output[0]
    
def create_cross_product_edges(uris_start: list[str], uris_end: list[str], mappping_node_uri_id  : dict[str, int]) -> list[tuple[int, int]]:
    return [(mappping_node_uri_id[s], mappping_node_uri_id[e]) for s in uris_start for e in uris_end]
        
def get_extra_args(initial_args: list[int], desc: list[tuple]) -> dict[int, str]:
    extra_args = {}
    for i,d in enumerate(desc):
        if i not in initial_args:
            extra_args[i] = d[0]
    return extra_args

def build_history_graph_of_geo_entities_from_template(
        duckdb_connection: duckdb.DuckDBPyConnection,
        template_sql_path: Path,
        context_template_sql: dict[str, str] = {}
    ) -> igraph.Graph:

    try:
        with open(template_sql_path, 'r', encoding='utf-8') as template_file:
            template_content = template_file.read()
    except Exception as e:
        raise RuntimeError(f"Failed to load template file {template_sql_path}") from e
    
    renderer = pystache.Renderer(escape=lambda s: s)
    try:
        rendered_str = renderer.render(template_content, context_template_sql)
    except Exception as e:
        raise RuntimeError(f"Failed to render template file {template_sql_path}") from e
    
    try:
        duckdb_connection.execute(rendered_str)
    except Exception as e:
        raise RuntimeError(f"Failed to execute query on duckdb") from e

    dict_geo_entity: dict[str, dict[str, str]] = {}
    dict_events: dict[str, tuple[list[str], list[str]]] = {}

    try:
        duckdb_connection.execute(f"FROM geo_entities_graph ;")
    except Exception as e:
        raise RuntimeError(f"Failed to execute query on geo_entities_graph view") from e
    
    j_uri = find_column_index_in_duckdb_output(label="uri", desc=duckdb_connection.description)
    j_label = find_column_index_in_duckdb_output(label="label", desc=duckdb_connection.description)
    j_insee_code = find_column_index_in_duckdb_output(label="insee_code", desc=duckdb_connection.description)
    j_start_date = find_column_index_in_duckdb_output(label="start_date", desc=duckdb_connection.description)
    j_end_date = find_column_index_in_duckdb_output(label="end_date", desc=duckdb_connection.description)
    j_start_event_uri = find_column_index_in_duckdb_output(label="start_event_uri", desc=duckdb_connection.description)
    j_end_event_uri = find_column_index_in_duckdb_output(label="end_event_uri", desc=duckdb_connection.description)
    
    extra_args = get_extra_args(
        initial_args=[j_uri, j_label, j_insee_code, j_start_date, j_end_date, j_start_event_uri, j_end_event_uri],
        desc=duckdb_connection.description
    )

    res_fetched = duckdb_connection.fetchone()
    while res_fetched is not None:
        dict_geo_entity[res_fetched[j_uri]] = {
            "name": res_fetched[j_uri],
            "entity_type": re.sub(
                pattern = r"[/].+",
                repl = "", 
                string=res_fetched[j_uri].replace('http://id.insee.fr/geo/', '')
            ),
            "uri": res_fetched[j_uri],
            "label": res_fetched[j_label],
            "insee_code": res_fetched[j_insee_code],
            "start_date": res_fetched[j_start_date],
            "end_date": res_fetched[j_end_date],
            "start_event_uri": res_fetched[j_start_event_uri],
            "end_event_uri": res_fetched[j_end_event_uri]
        }
        for k, v in extra_args.items():
            dict_geo_entity[res_fetched[j_uri]][v] = res_fetched[k]
        
        start_event = res_fetched[j_start_event_uri]
        end_event = res_fetched[j_end_event_uri]
        if start_event in dict_events:
            dict_events[start_event][1].append(res_fetched[j_uri])
        else:
            dict_events[start_event] = ([], [res_fetched[j_uri]])
        
        if end_event:
            if end_event in dict_events:
                dict_events[end_event][0].append(res_fetched[j_uri])
            else:
                dict_events[end_event] = ([res_fetched[j_uri]], [])
        res_fetched = duckdb_connection.fetchone()

    mappping_node_uri_id  : dict[str, int] = {}
    mappping_node_id_uri  : dict[int, str] = {}

    for i, uri in enumerate(list(dict_geo_entity.keys())):
        mappping_node_uri_id[uri] = i
        mappping_node_id_uri[i] = uri


    vertices : list[dict["str", "str"]] = [dict_geo_entity[mappping_node_id_uri[i]] for i in range(len(mappping_node_id_uri))]
    edges : list[tuple[int, int]] =  [el for v in dict_events.values() for el in create_cross_product_edges(v[0], v[1], mappping_node_uri_id)]    
    g = igraph.Graph(directed=True)
    vertices_attributes = {
            "name": [v["name"] for v in vertices],
            "entity_type": [v["entity_type"] for v in vertices],
            "uri": [v["uri"] for v in vertices],
            "label": [v["label"] for v in vertices],
            "insee_code": [v["insee_code"] for v in vertices],
            "start_date": [v["start_date"] for v in vertices],
            "end_date": [v["end_date"] for v in vertices],
            "start_event_uri": [v["start_event_uri"] for v in vertices],
            "end_event_uri": [v["end_event_uri"] for v in vertices]
    }
    for k in extra_args.values():
        vertices_attributes[k] = [v[k] for v in vertices]

    g.add_vertices(
        n = len(vertices),
        attributes=vertices_attributes
    )
    g.add_edges(
        es=edges,
        attributes={"date": [dict_geo_entity[mappping_node_id_uri[el[1]]]["start_date"] for el in edges]}
    )
    return g


def build_history_graph_of_geo_entities(
        duckdb_connection: duckdb.DuckDBPyConnection,
        current_stored_data: InfoCurrentStoredData,
        tables: list[str],
        keep_columns: Optional[list[str]] = None
    ) -> igraph.Graph:
    table_name='geo_entities_graph'
    tables_list_metadata: list[GeoCSVFileMetadata] = []
    colnames : dict[str, ColumnDataType] = {}
    for t in tables:
        if t in current_stored_data.data:
            d = current_stored_data.get_data(t)
            if isinstance(d, GeoCSVFileMetadata):
                tables_list_metadata.append(d)
                for col in d.colnames:
                    if col.name in colnames:
                        if colnames[col.name] == col:
                            if keep_columns is None:
                                colnames[col.name] = col
                            elif col.name in keep_columns:
                                colnames[col.name] = col
                        else:
                            raise RuntimeError(f"Column {col.name} already exists in colnames with a different specification")
                    else:
                        if keep_columns is None:
                            colnames[col.name] = col
                        elif col.name in keep_columns:
                            colnames[col.name] = col
            else:
                raise RuntimeError(f"Table {t} is not a GeoCSVFileMetadata")
        else:
            raise RuntimeError(f"Table {t} not found in current_stored_data")
        
        sql_query = """
        CREATE OR REPLACE VIEW {table_name} AS (
            {query}
        );
        """.format(
            table_name=table_name,
            query=" UNION ALL BY NAME ".join([t.get_duckdb_sql_import_query(keep_colnames=list(colnames.keys())) for t in tables_list_metadata])
        )
    
    try:
        duckdb_connection.execute(sql_query)
    except Exception as e:
        raise RuntimeError(f"Failed to execute query on duckdb") from e

    dict_geo_entity: dict[str, dict[str, str]] = {}
    dict_events: dict[str, tuple[list[str], list[str]]] = {}

    try:
        duckdb_connection.execute(f"FROM {table_name} ;")
    except Exception as e:
        raise RuntimeError(f"Failed to execute query on geo_entities_graph view") from e
    
    j_uri = find_column_index_in_duckdb_output(label="uri", desc=duckdb_connection.description)
    j_label = find_column_index_in_duckdb_output(label="label", desc=duckdb_connection.description)
    j_insee_code = find_column_index_in_duckdb_output(label="insee_code", desc=duckdb_connection.description)
    j_start_date = find_column_index_in_duckdb_output(label="start_date", desc=duckdb_connection.description)
    j_end_date = find_column_index_in_duckdb_output(label="end_date", desc=duckdb_connection.description)
    j_start_event_uri = find_column_index_in_duckdb_output(label="start_event_uri", desc=duckdb_connection.description)
    j_end_event_uri = find_column_index_in_duckdb_output(label="end_event_uri", desc=duckdb_connection.description)
    
    extra_args = get_extra_args(
        initial_args=[j_uri, j_label, j_insee_code, j_start_date, j_end_date, j_start_event_uri, j_end_event_uri],
        desc=duckdb_connection.description
    )

    res_fetched = duckdb_connection.fetchone()
    while res_fetched is not None:
        dict_geo_entity[res_fetched[j_uri]] = {
            "name": res_fetched[j_uri],
            "entity_type": re.sub(
                pattern = r"[/].+",
                repl = "", 
                string=res_fetched[j_uri].replace('http://id.insee.fr/geo/', '')
            ),
            "uri": res_fetched[j_uri],
            "label": res_fetched[j_label],
            "insee_code": res_fetched[j_insee_code],
            "start_date": res_fetched[j_start_date],
            "end_date": res_fetched[j_end_date],
            "start_event_uri": res_fetched[j_start_event_uri],
            "end_event_uri": res_fetched[j_end_event_uri]
        }
        for k, v in extra_args.items():
            dict_geo_entity[res_fetched[j_uri]][v] = res_fetched[k]
        
        start_event = res_fetched[j_start_event_uri]
        end_event = res_fetched[j_end_event_uri]
        if start_event in dict_events:
            dict_events[start_event][1].append(res_fetched[j_uri])
        else:
            dict_events[start_event] = ([], [res_fetched[j_uri]])
        
        if end_event:
            if end_event in dict_events:
                dict_events[end_event][0].append(res_fetched[j_uri])
            else:
                dict_events[end_event] = ([res_fetched[j_uri]], [])
        res_fetched = duckdb_connection.fetchone()

    mappping_node_uri_id  : dict[str, int] = {}
    mappping_node_id_uri  : dict[int, str] = {}

    for i, uri in enumerate(list(dict_geo_entity.keys())):
        mappping_node_uri_id[uri] = i
        mappping_node_id_uri[i] = uri


    vertices : list[dict["str", "str"]] = [dict_geo_entity[mappping_node_id_uri[i]] for i in range(len(mappping_node_id_uri))]
    edges : list[tuple[int, int]] =  [el for v in dict_events.values() for el in create_cross_product_edges(v[0], v[1], mappping_node_uri_id)]    
    g = igraph.Graph(directed=True)
    vertices_attributes = {
            "name": [v["name"] for v in vertices],
            "entity_type": [v["entity_type"] for v in vertices],
            "uri": [v["uri"] for v in vertices],
            "label": [v["label"] for v in vertices],
            "insee_code": [v["insee_code"] for v in vertices],
            "start_date": [v["start_date"] for v in vertices],
            "end_date": [v["end_date"] for v in vertices],
            "start_event_uri": [v["start_event_uri"] for v in vertices],
            "end_event_uri": [v["end_event_uri"] for v in vertices]
    }
    for k in extra_args.values():
        vertices_attributes[k] = [v[k] for v in vertices]

    g.add_vertices(
        n = len(vertices),
        attributes=vertices_attributes
    )
    g.add_edges(
        es=edges,
        attributes={"date": [dict_geo_entity[mappping_node_id_uri[el[1]]]["start_date"] for el in edges]}
    )
    return g
