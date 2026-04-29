import duckdb
from pathlib import Path
import logging
import re
from rapidfuzz import fuzz
from typing import Optional
import unicodedata
import csv
import igraph

from ..metadata import InfoCurrentStoredData, GeoCSVFileMetadata, StringListColumnDataType, StringColumnDataType
from ..graph.graph_event import build_history_graph_of_geo_entities

def get_uri(vertex, uri_attr: str = 'uri') -> str:
    uri = None
    try:
        uri = vertex[uri_attr]
    except:
        pass
    if isinstance(uri, str):
        return uri
    else:
        raise RuntimeError("The uri is not a string")
    
def get_parent_uri(vertex, parent_uri_attr: str = 'parent_uri', sep: str = '|') -> list[str]:
    parent_uri = None
    try:
        parent_uri = vertex[parent_uri_attr]
    except:
        pass
    if isinstance(parent_uri, str):
        return parent_uri.split(sep=sep)
    else:
        raise RuntimeError("The parent_uri_attr is not a string")
    
def normalize_location_label(s: str) -> str:
    STOPWORDS = {'territoire', 'departement', 'l', 'le', 'la', 'de', 'd', 'a', 'les', 'en', 'au', 'aux', 'une', 'sur', 'du'}
    s = s.lower()
    s = unicodedata.normalize('NFD', s)
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    s = s.replace("-", " ")
    s = s.replace("'", " ")
    s = s.replace("&", " ")
    s = s.replace(";", " ")
    s = re.sub(r'[^a-z ]', '', s)
    s = re.sub(r'[ ]+', ' ', s)
    words = s.split()
    words = [w for w in words if w not in STOPWORDS]
    return " ".join(words)

def is_label_location_similar(s1: str, s2: str) -> bool:
    try:
        score = fuzz.ratio(normalize_location_label(s1), normalize_location_label(s2))
        if score >= 90:
            return True
    except:
        pass
    return False

def is_france_metropolitaine(parent_vertices) -> bool:
    for v in parent_vertices:
        try:
            is_france_metropolitaine = v['is_france_metropolitaine']
            if isinstance(is_france_metropolitaine, bool):
                if is_france_metropolitaine:
                    return True
        except:
            pass
    return False

def is_algerie(parent_vertices) -> bool:
    for v in parent_vertices:
        is_france_metropolitaine : Optional[bool] = None
        insee_code: Optional[str] = None
        label: Optional[str] = None
        try:
            is_france_metropolitaine = v['is_france_metropolitaine']                   
        except:
            pass
        try:
            insee_code = v['insee_code']                   
        except:
            pass
        try:
            label = v['label']                   
        except:
            pass
        if isinstance(is_france_metropolitaine, bool) and isinstance(insee_code, str) and isinstance(label, str):
            if not(is_france_metropolitaine) and insee_code in ['91', '91000', '91352', '99352'] and is_label_location_similar(label, 'Alger'):
                return True
            if not(is_france_metropolitaine) and insee_code in ['92', '92000', '92352', '99352'] and is_label_location_similar(label, 'Oran'):
                return True
            if not(is_france_metropolitaine) and insee_code in ['93', '93000', '93352', '99352'] and is_label_location_similar(label, 'Constantine'):
                return True
            if not(is_france_metropolitaine) and insee_code in ['94', '94000', '94352', '99352'] and is_label_location_similar(label, 'Territoires du sud de l''Algérie'):
                return True
    return False

def is_maroc(parent_vertices) -> bool:
    for v in parent_vertices:
        geo_entity_type : Optional[str] = None
        insee_code: Optional[str] = None
        label: Optional[str] = None
        try:
            uri = v['uri']
            geo_entity_type = re.sub(
                pattern = r"[/].+",
                repl = "", 
                string=uri.replace('http://id.insee.fr/geo/', '')
            )          
        except:
            pass
        try:
            insee_code = v['insee_code']                   
        except:
            pass
        try:
            label = v['label']                   
        except:
            pass
        if isinstance(geo_entity_type, str) and isinstance(insee_code, str) and isinstance(label, str):
            if geo_entity_type == 'collectiviteDOutreMer' and insee_code in ['95', '95000', '95350', '99350'] and is_label_location_similar(label, 'Maroc'):
                return True
    return False

def is_tunisie(parent_vertices) -> bool:
    for v in parent_vertices:
        geo_entity_type : Optional[str] = None
        insee_code: Optional[str] = None
        label: Optional[str] = None
        try:
            uri = v['uri']
            geo_entity_type = re.sub(
                pattern = r"[/].+",
                repl = "", 
                string=uri.replace('http://id.insee.fr/geo/', '')
            )          
        except:
            pass
        try:
            insee_code = v['insee_code']                   
        except:
            pass
        try:
            label = v['label']                   
        except:
            pass
        if isinstance(geo_entity_type, str) and isinstance(insee_code, str) and isinstance(label, str):
            if geo_entity_type == 'collectiviteDOutreMer' and insee_code in ['96', '96000', '96351', '99351'] and is_label_location_similar(label, 'Tunisie'):
                return True
    return False

def is_guadeloupe(parent_vertices) -> bool:
    for v in parent_vertices:
        insee_code: Optional[str] = None
        label: Optional[str] = None
        try:
            insee_code = v['insee_code']                   
        except:
            pass
        try:
            label = v['label']                   
        except:
            pass
        if isinstance(insee_code, str) and isinstance(label, str):
            if insee_code in ['98702', '97100', '971'] and is_label_location_similar(label, 'Guadeloupe'):
                return True
    return False

def is_martinique(parent_vertices) -> bool:
    for v in parent_vertices:
        insee_code: Optional[str] = None
        label: Optional[str] = None
        try:
            insee_code = v['insee_code']                   
        except:
            pass
        try:
            label = v['label']                   
        except:
            pass
        if isinstance(insee_code, str) and isinstance(label, str):
            if insee_code in ['98703', '97200', '972'] and is_label_location_similar(label, 'Martinique'):
                return True
    return False

def is_guyane(parent_vertices) -> bool:
    for v in parent_vertices:
        insee_code: Optional[str] = None
        label: Optional[str] = None
        try:
            insee_code = v['insee_code']                   
        except:
            pass
        try:
            label = v['label']                   
        except:
            pass
        if isinstance(insee_code, str) and isinstance(label, str):
            if insee_code in ['98704', '97300', '973'] and is_label_location_similar(label, 'Guyane'):
                return True
    return False

def is_reunion(parent_vertices) -> bool:
    for v in parent_vertices:
        insee_code: Optional[str] = None
        label: Optional[str] = None
        try:
            insee_code = v['insee_code']                   
        except:
            pass
        try:
            label = v['label']                   
        except:
            pass
        if isinstance(insee_code, str) and isinstance(label, str):
            if insee_code in ['98405', '97400', '974'] and is_label_location_similar(label, 'Réunion'):
                return True
    return False

def is_mayotte(parent_vertices) -> bool:
    for v in parent_vertices:
        insee_code: Optional[str] = None
        label: Optional[str] = None
        try:
            insee_code = v['insee_code']                   
        except:
            pass
        try:
            label = v['label']                   
        except:
            pass
        if isinstance(insee_code, str) and isinstance(label, str):
            if insee_code in ['976', '97600', '985', '98500', '98402', '98402'] and is_label_location_similar(label, 'Mayotte'):
                return True
    return False

def is_saint_pierre_et_miquelon(parent_vertices) -> bool:
    for v in parent_vertices:
        insee_code: Optional[str] = None
        label: Optional[str] = None
        try:
            insee_code = v['insee_code']                   
        except:
            pass
        try:
            label = v['label']                   
        except:
            pass
        if isinstance(insee_code, str) and isinstance(label, str):
            if insee_code in ['975', '97500', '98701'] and is_label_location_similar(label, 'Saint-Pierre-et-Miquelon'):
                return True
    return False

def is_saint_barthelemy(parent_vertices) -> bool:
    for v in parent_vertices:
        insee_code: Optional[str] = None
        label: Optional[str] = None
        try:
            insee_code = v['insee_code']                   
        except:
            pass
        try:
            label = v['label']                   
        except:
            pass
        if isinstance(insee_code, str) and isinstance(label, str):
            if insee_code in ['977', '97700'] and is_label_location_similar(label, 'Saint-Barthélemy'):
                return True
    return False

def is_saint_martin(parent_vertices) -> bool:
    for v in parent_vertices:
        insee_code: Optional[str] = None
        label: Optional[str] = None
        try:
            insee_code = v['insee_code']                   
        except:
            pass
        try:
            label = v['label']                   
        except:
            pass
        if isinstance(insee_code, str) and isinstance(label, str):
            if insee_code in ['978', '97800'] and is_label_location_similar(label, 'Saint-Martin'):
                return True
    return False


def is_polynesie_française(parent_vertices) -> bool:
    for v in parent_vertices:
        insee_code: Optional[str] = None
        label: Optional[str] = None
        try:
            insee_code = v['insee_code']                   
        except:
            pass
        try:
            label = v['label']                   
        except:
            pass
        if isinstance(insee_code, str) and isinstance(label, str):
            if insee_code in ['987', '98700'] and is_label_location_similar(label, 'Polynésie française'):
                return True
    return False


def is_nouvelle_caledonie(parent_vertices) -> bool:
    for v in parent_vertices:
        insee_code: Optional[str] = None
        label: Optional[str] = None
        try:
            insee_code = v['insee_code']                   
        except:
            pass
        try:
            label = v['label']                   
        except:
            pass
        if isinstance(insee_code, str) and isinstance(label, str):
            if insee_code in ['988', '98800'] and is_label_location_similar(label, 'Nouvelle-Calédonie'):
                return True
    return False

def typologize_communes(
    duckdb_connection: duckdb.DuckDBPyConnection,
    current_stored_data: InfoCurrentStoredData,
    output_dir : Path
) -> None: 
    
    parent_uri_colname_label = 'parent_uri'
    uri_colname_label = 'uri'
    

    logging.info("Add column location_typology to the communes table.")   
    communes_metadata=current_stored_data.get_data(key='insee_communes')
    communes_path = communes_metadata.path
    if isinstance(communes_metadata, GeoCSVFileMetadata):
        communes_colnames = communes_metadata.colnames
    else:
        raise RuntimeError("The metadata for the communes table is not a GeoCSVFileMetadata")
    
    graph = build_history_graph_of_geo_entities(
        duckdb_connection=duckdb_connection,
        current_stored_data=current_stored_data,
        tables=['insee_communes', 'insee_departements', 'insee_collectivites_outremer']
    )

    
    parent_uri_colname = None
    uri_colname = None
    for x in communes_colnames:
        if x.name == parent_uri_colname_label:
            parent_uri_colname = x
        elif x.name == uri_colname_label:
            uri_colname = x
    if parent_uri_colname is None:
        raise RuntimeError(f"The column '{parent_uri_colname_label}' is not found in the communes table.")
    if not isinstance(parent_uri_colname, StringListColumnDataType):
        raise RuntimeError(f"The column '{parent_uri_colname_label}' is not a StringListColumnDataType")
    
    if uri_colname is None:
        raise RuntimeError(f"The column '{uri_colname}' is not found in the communes table.")
    if not isinstance(uri_colname, StringColumnDataType):
        raise RuntimeError(f"The column '{uri_colname}' is not a StringColumnDataType")
    
    location_category_dict: dict[str, tuple[str, igraph.Vertex]] = {}
    for v in graph.vs:
        uri = get_uri(vertex=v, uri_attr=uri_colname.name)
        entity_type=re.sub(
                pattern = r"[/].+",
                repl = "", 
                string=uri.replace('http://id.insee.fr/geo/', '')
        )
        if entity_type == "commune":
            try:
                parent_uri_list = get_parent_uri(vertex=v, parent_uri_attr=parent_uri_colname.name, sep=parent_uri_colname.sep) 
            except Exception as e:
                raise RuntimeError(f"Unable to get the parent_uri for the uri {uri}") from e

            parent_vertices = []
            for parent_uri in parent_uri_list:
                try:
                    parent_vertices.append(graph.vs.find(name=parent_uri))
                except:
                    raise RuntimeError(f"Unable to find the parent vertex for the uri {uri} with parent_uri={parent_uri}")
                
            typology_dict : dict[str, bool] = {
                'france_metropolitaine': is_france_metropolitaine(parent_vertices),
                'algerie': is_algerie(parent_vertices),
                'maroc': is_maroc(parent_vertices),
                'tunisie': is_tunisie(parent_vertices),
                'guadeloupe': is_guadeloupe(parent_vertices),
                'martinique': is_martinique(parent_vertices),
                'guyane': is_guyane(parent_vertices),
                'reunion': is_reunion(parent_vertices),
                'mayotte': is_mayotte(parent_vertices),
                'saint_pierre_et_miquelon': is_saint_pierre_et_miquelon(parent_vertices),
                'saint_barthelemy': is_saint_barthelemy(parent_vertices),
                'saint_martin': is_saint_martin(parent_vertices),
                'polynesie_française': is_polynesie_française(parent_vertices),
                'nouvelle_caledonie': is_nouvelle_caledonie(parent_vertices)
            }
            true_items = [k for k, v in typology_dict.items() if v]
            if len(true_items) == 0:
                raise RuntimeError(f"Unable to compute the location category for the commune with the URI {uri} : {typology_dict}")
            elif len(true_items) > 1:
                raise RuntimeError(f"Several location categories may apply for the commune with the URI {uri} : {typology_dict}")
            else:
                location_category_dict[uri] = (true_items[0],v)
        
    output_csv_path = output_dir / communes_path.name
    new_col = StringColumnDataType(name='location_typology')
    output_dir.mkdir(parents=True, exist_ok=True)
    with open(output_csv_path, mode="w", newline="", encoding="utf-8") as f_communes:
        writer_communes = csv.DictWriter(f_communes, fieldnames=[x.name for x in communes_colnames]+[new_col.name])
        writer_communes.writeheader()
        for el in location_category_dict.values():
            v = el[1]
            row_output = {}
            for col in communes_colnames:
                try:
                    row_output[col.name] = v[col.name]
                except KeyError as e:
                    raise e
            row_output[new_col.name] = el[0]
            writer_communes.writerow(row_output)
    current_stored_data.replace_data(
        key='insee_communes',
        value=GeoCSVFileMetadata(
            path=output_csv_path,
            header=True,
            delim=',',
            encoding='utf-8',
            colnames=communes_colnames+[new_col]
        )
    )
    logging.info("The column location_typology has been added to the communes table.")
