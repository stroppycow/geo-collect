COPY (
    SELECT * REPLACE (
        {{modif_event_uri_sql}} AS {{event_uri}},
        {{modif_date_sql}} AS {{date_name}},
        {{modif_date_count_sql}} AS {{date_count_name}}
    )
    FROM {{view_name}}
    {{filter_condition}}
) TO '{{output_path}}' (FORMAT CSV, HEADER TRUE) ;
