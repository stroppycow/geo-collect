COPY (
    SELECT * REPLACE (
        {{modif_start_date}} AS start_date
    )
    FROM {{view_name}}
    {{filter_condition}}
) TO '{{output_path}}' (FORMAT CSV, HEADER TRUE) ;
