COPY (
    SELECT * REPLACE (
        {{uri_sql}} AS uri,
        {{insee_code_sql}} AS insee_code,
        {{label_sql}} AS label,
        {{article_code_sql}} AS article_code,
        {{parent_uri_sql}} AS parent_uri,
        {{start_event_uri_sql}} AS start_event_uri,
        {{end_event_uri}} AS end_event_uri,
        {{start_date_sql}} AS start_date,
        {{end_date_sql}} AS end_date,
        {{parent_uri_count_sql}} AS parent_uri_count,
        {{start_date_count_sql}} AS start_date_count,
        {{end_date_count_sql}} AS end_date_count
    )
    FROM {{view_name}}
    {{filter_condition}}
    {{add_rows_sql}}
) TO '{{output_path}}' (FORMAT CSV, HEADER TRUE) ;
