SELECT row_num, uri, col
FROM (
    SELECT row_number() OVER () as row_num, coalesce(uri, '') as uri, coalesce({{colname}}, '') as col
    FROM {{view_name}}
)
WHERE not(regexp_matches(col, '{{pattern}}'))
LIMIT 1 ;
