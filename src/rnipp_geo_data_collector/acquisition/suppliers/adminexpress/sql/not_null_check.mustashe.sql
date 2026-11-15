SELECT row_num
FROM (
    SELECT row_number() OVER () as row_num, {{colname}}
    FROM {{view_name}}
)
WHERE {{colname}} IS NULL
LIMIT 1 ;
