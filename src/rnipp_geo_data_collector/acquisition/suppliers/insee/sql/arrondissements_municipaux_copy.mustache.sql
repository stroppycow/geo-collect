COPY (
    SELECT
        uri,
        insee_code,
        label,
        article_code,
        insee_codes_communes_parent,
        start_date,
        end_date,
        insee_codes_communes_parent_count,
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
            'insee_codes_communes_parent': 'VARCHAR',
            'start_date': 'DATE',
            'end_date': 'DATE',
            'insee_codes_communes_parent_count': 'INTEGER',
            'start_date_count': 'INTEGER',
            'end_date_count': 'INTEGER'
        }
    )
) TO '{{output_path}}' (FORMAT CSV, HEADER TRUE) ;
