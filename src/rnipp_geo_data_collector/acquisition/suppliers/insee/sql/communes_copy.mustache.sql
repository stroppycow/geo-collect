COPY (
    SELECT
        uri,
        insee_code,
        label,
        article_code,
        depatement_insee_code,
        start_date,
        end_date,
        depatement_insee_code_count,
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
            'depatement_insee_code': 'VARCHAR',
            'start_date': 'DATE',
            'end_date': 'DATE',
            'depatement_insee_code_count': 'INTEGER',
            'start_date_count': 'INTEGER',
            'end_date_count': 'INTEGER'
        }
    )
) TO '{{output_path}}' (FORMAT CSV, HEADER TRUE) ;
