import duckdb
from pathlib import Path
import csv
import igraph
import logging

from ..metadata import InfoCurrentStoredData, GeoCSVFileMetadata
from ..graph.graph_event import build_history_graph_of_geo_entities

def _correct_record_metropolitan_france_dummy(g: igraph.Graph, id: int) -> bool:
    accessibles = g.subcomponent(id, mode="ALL")
    for v in accessibles:
        if g.vs[v]['is_france_metropolitaine']:
            return True
    return False

def correct_metropolitan_france_dummy(
        duckdb_connection: duckdb.DuckDBPyConnection,
        current_stored_data: InfoCurrentStoredData,
        output_dir : Path
) -> None:
    """
    Correct the dummy value for the metropolitan France in the departements table.
    """
    logging.info("Correcting the dummy value for the metropolitan France in the departements table.")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    departements_metadata=current_stored_data.get_data(key='insee_departements')
    download_departements_path = departements_metadata.path
    if isinstance(departements_metadata, GeoCSVFileMetadata):
        download_departements_colnames = departements_metadata.colnames
    else:
        raise RuntimeError("The metadata for the departements table is not a GeoCSVFileMetadata")
    
    graph = build_history_graph_of_geo_entities(
        duckdb_connection=duckdb_connection,
        current_stored_data=current_stored_data,
        tables=['insee_departements']
    )
    output_csv_path = output_dir / download_departements_path.name
    with open(output_csv_path, mode="w", newline="", encoding="utf-8") as f_departements:
        writer_departements = csv.DictWriter(f_departements, fieldnames=[x.name for x in download_departements_colnames])
        writer_departements.writeheader()
        for v in graph.vs:
            row_output = {}
            for col in download_departements_colnames:
                if col.name == 'is_france_metropolitaine':
                    row_output[col.name] = _correct_record_metropolitan_france_dummy(g=graph, id=v.index)
                else:
                    try:
                        row_output[col.name] = v[col.name]
                    except KeyError as e:
                        raise e
            writer_departements.writerow(row_output)
    current_stored_data.replace_data(
        key='insee_departements',
        value=GeoCSVFileMetadata(
            path=output_csv_path,
            header=True,
            delim=',',
            encoding='utf-8',
            colnames=departements_metadata.colnames
        )
    )
    
    logging.info("New departements table created at {path}".format(path=str(output_csv_path.resolve())))
    

    
