SELECT key_value, nb
FROM (
    SELECT coalesce(CAST({{colname}} AS VARCHAR), '<NULL>') as key_value, count(*) as nb
    FROM {{view_name}}
    GROUP BY key_value
    HAVING count(*) > 1
)
LIMIT 1 ;
