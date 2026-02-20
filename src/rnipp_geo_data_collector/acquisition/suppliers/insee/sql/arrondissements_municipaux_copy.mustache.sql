COPY (
    SELECT
        uri,
        insee_code,
        label,
        article_code,
        parent_uri,
        start_event_uri,
        end_event_uri,
        start_date,
        end_date,
        parent_uri_count,
        start_date_count,
        end_date_count
    FROM read_csv(
        '{{path}}',
        delim = ',',
        header = true,
        columns = {
            'uri': 'VARCHAR',
            'insee_code': 'VARCHAR',
            'label': 'VARCHAR',
            'article_code': 'VARCHAR',
            'parent_uri': 'VARCHAR',
            'start_event_uri': 'VARCHAR',
            'end_event_uri': 'VARCHAR',
            'start_date': 'DATE',
            'end_date': 'DATE',
            'parent_uri_count': 'INTEGER',
            'start_date_count': 'INTEGER',
            'end_date_count': 'INTEGER'
        }
    )
) TO '{{output_path}}' (FORMAT CSV, HEADER TRUE) ;
