COPY (
    SELECT * REPLACE (
        {{parent_field_value}} AS {{parent_field_name}},
        1::INTEGER AS {{count_field_name}}
    )
    FROM {{view_name}}
    {{filter_condition}}
) TO '{{output_path}}' (FORMAT CSV, HEADER TRUE) ;
