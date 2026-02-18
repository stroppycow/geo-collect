COPY (
    SELECT * REPLACE (
        {{modif_uri_sql}} AS uri
    )
    FROM {{view_name}}
    {{filter_condition}}
) TO '{{output_path}}' (FORMAT CSV, HEADER TRUE) ;
