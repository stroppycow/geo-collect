SELECT row_num, uri, col
FROM (
    SELECT row_number() OVER () as row_num, coalesce(uri, '') as uri, {{colname}} as col
    FROM {{view_name}}
)
WHERE col is null
LIMIT 1 ;
