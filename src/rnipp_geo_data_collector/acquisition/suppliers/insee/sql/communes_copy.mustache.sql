COPY (
    SELECT
        uri,
        insee_code,
        label,
        article_code,
        parent_arrondissement_uri,
        parent_departement_uri,
        parent_com_uri,
        start_date,
        end_date,
        parent_arrondissement_count,
        parent_departement_count,
        parent_com_count,
        start_date_count,
        end_date_count
    FROM read_csv(
        '{{input_path}}',
        delim = ',',
        header = true,
        columns = {
            'uri': 'VARCHAR',
            'insee_code': 'VARCHAR',
            'label': 'VARCHAR',
            'article_code': 'VARCHAR',
            'parent_arrondissement_uri': 'VARCHAR',
            'parent_departement_uri': 'VARCHAR',
            'parent_com_uri': 'VARCHAR',
            'start_date': 'DATE',
            'end_date': 'DATE',
            'parent_arrondissement_count': 'INTEGER',
            'parent_departement_count': 'INTEGER',
            'parent_com_count': 'INTEGER',
            'start_date_count': 'INTEGER',
            'end_date_count': 'INTEGER'
        }
    )
) TO '{{output_path}}' (FORMAT CSV, HEADER TRUE) ;
