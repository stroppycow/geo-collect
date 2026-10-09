import csv
import logging
from pathlib import Path

import duckdb
import igraph

from ..graph.graph_event import build_history_graph_of_geo_entities
from ..metadata import GeoCSVFileMetadata, InfoCurrentStoredData

logger = logging.getLogger(__name__)


def _correct_record_metropolitan_france_dummy(g: igraph.Graph, id: int) -> bool:
    accessibles = g.subcomponent(id, mode="ALL")
    for v in accessibles:
        if g.vs[v]["is_france_metropolitaine"]:
            return True
    return False


def correct_metropolitan_france_dummy(
    duckdb_connection: duckdb.DuckDBPyConnection,
    current_stored_data: InfoCurrentStoredData,
    output_dir: Path,
) -> None:
    """
    Correct the dummy value for the metropolitan France in the departements table.
    """
    logger.info(
        "Correcting the dummy value for the metropolitan France in the departements table."
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    departements_metadata = current_stored_data.get_data(key="insee_departements")
    download_departements_path = departements_metadata.path
    if isinstance(departements_metadata, GeoCSVFileMetadata):
        download_departements_colnames = departements_metadata.colnames
    else:
        raise TypeError(
            "The metadata for the departements table is not a GeoCSVFileMetadata"
        )

    graph = build_history_graph_of_geo_entities(
        duckdb_connection=duckdb_connection,
        current_stored_data=current_stored_data,
        tables=["insee_departements"],
    )
    output_csv_path = output_dir / download_departements_path.name
    with open(
        output_csv_path, mode="w", newline="", encoding="utf-8"
    ) as f_departements:
        writer_departements = csv.DictWriter(
            f_departements, fieldnames=[x.name for x in download_departements_colnames]
        )
        writer_departements.writeheader()
        for v in graph.vs:
            row_output = {}
            for col in download_departements_colnames:
                if col.name == "is_france_metropolitaine":
                    row_output[col.name] = _correct_record_metropolitan_france_dummy(
                        g=graph, id=v.index
                    )
                else:
                    try:
                        row_output[col.name] = v[col.name]
                    except KeyError as e:
                        raise RuntimeError(
                            f"""
                            Missing key '{col.name}' in the departements table
                            for the geo entity at graph vertex {v.index}
                            """
                        ) from e
            writer_departements.writerow(row_output)
    current_stored_data.replace_data(
        key="insee_departements",
        value=GeoCSVFileMetadata(
            path=output_csv_path,
            header=True,
            delim=",",
            encoding="utf-8",
            colnames=departements_metadata.colnames,
        ),
    )

    logger.info(f"New departements table created at {output_csv_path.resolve()!s}")
