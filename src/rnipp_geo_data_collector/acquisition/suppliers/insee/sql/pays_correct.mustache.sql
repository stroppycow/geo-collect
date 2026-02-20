COPY (
    SELECT * REPLACE (
        {{uri_sql}} AS uri,
        {{insee_code_sql}} AS insee_code,
        {{label_sql}} AS label,
        {{article_code_sql}} AS article_code,
        {{long_label_sql}} AS long_label,
        {{iso3166alpha2_code_sql}} AS iso3166alpha2_code,
        {{iso3166alpha3_code_sql}} AS iso3166alpha3_code,
        {{iso3166num_code_sql}} AS iso3166num_code,
        {{start_event_uri_sql}} AS start_event_uri,
        {{end_event_uri_sql}} AS end_event_uri,
        {{start_date_sql}} AS start_date,
        {{end_date_sql}} AS end_date,
        {{start_date_count_sql}} AS start_date_count,
        {{end_date_count_sql}} AS end_date_count
    )
    FROM {{view_name}}
    {{filter_condition}}
    {{add_rows_sql}}
) TO '{{output_path}}' (FORMAT CSV, HEADER TRUE) ;
