COPY (
    SELECT * REPLACE (
        {{modif_insee_code_sql}} AS insee_code
    )
    FROM {{view_name}}
    {{filter_condition}}
) TO '{{output_path}}' (FORMAT CSV, HEADER TRUE) ;
