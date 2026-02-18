COPY (
    SELECT
        uri,
        date,
        added_entities,
        removed_entities,
        added_entities_count,
        removed_entities_count
    FROM read_csv(
        '{{input_path}}',
        delim = ',',
        header = true,
        columns = {
            'uri': 'VARCHAR',
            'date': 'DATE',
            'added_entities': 'VARCHAR',
            'removed_entities': 'VARCHAR',
            'added_entities_count': 'INTEGER',
            'removed_entities_count': 'INTEGER'
        }
    )
) TO '{{output_path}}' (FORMAT CSV, HEADER TRUE) ;
