COPY (
    SELECT
        uri,
        insee_code,
        label,
        article_code,
        long_label,
        iso3166alpha2_code,
        iso3166alpha3_code,
        iso3166num_code,
        start_date,
        end_date,
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
            'long_label': 'VARCHAR',
            'iso3166alpha2_code': 'VARCHAR',
            'iso3166alpha3_code': 'VARCHAR',
            'iso3166num_code': 'VARCHAR',
            'start_date': 'DATE',
            'end_date': 'DATE',
            'start_date_count': 'INTEGER',
            'end_date_count': 'INTEGER'
        }
    )
) TO '{{output_path}}' (FORMAT CSV, HEADER TRUE) ;
