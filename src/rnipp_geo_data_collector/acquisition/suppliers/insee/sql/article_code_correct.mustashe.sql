COPY (
    SELECT * REPLACE (
        {{modif_article_code_sql}} AS article_code
    )
    FROM {{view_name}}
    {{filter_condition}}
) TO '{{output_path}}' (FORMAT CSV, HEADER TRUE) ;
