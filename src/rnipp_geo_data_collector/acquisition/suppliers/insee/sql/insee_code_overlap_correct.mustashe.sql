COPY (
    SELECT * REPLACE (
        {{modif_insee_code_sql}} AS insee_code,
        {{modif_start_date_sql}} AS start_date,
        {{modif_end_date_sql}} AS end_date
    )
    FROM {{view_name}}
    {{filter_condition}}
) TO '{{output_path}}' (FORMAT CSV, HEADER TRUE) ;
