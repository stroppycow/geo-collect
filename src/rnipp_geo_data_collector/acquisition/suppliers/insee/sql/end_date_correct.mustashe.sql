COPY (
    SELECT * REPLACE (
        {{modif_end_date}} AS end_date
    )
    FROM {{view_name}}
    {{filter_condition}}
) TO '{{output_path}}' (FORMAT CSV, HEADER TRUE) ;
