SELECT row_num, col
FROM (
    SELECT row_number() OVER () as row_num, coalesce({{colname}}, '') as col
    FROM {{view_name}}
)
WHERE not(regexp_matches(col, '{{pattern}}'))
LIMIT 1 ;
