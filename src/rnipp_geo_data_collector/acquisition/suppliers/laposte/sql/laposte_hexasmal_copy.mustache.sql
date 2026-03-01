COPY (
    SELECT
        insee_code,
        name,
        postal_code,
        delivery_label,
        associated_name
    FROM read_csv(
        '{{input_path}}',
        delim = ';',
        skip = 1,
        header = false,
        columns = {
            'insee_code': 'VARCHAR',
            'name': 'VARCHAR',
            'postal_code': 'VARCHAR',
            'delivery_label': 'VARCHAR',
            'associated_name': 'VARCHAR'
        }
    )
) TO '{{output_path}}' (FORMAT CSV, HEADER TRUE) ;
