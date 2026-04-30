SELECT group_col, col
FROM (
    SELECT group_col, count(DISTINCT col) as nb, list_distinct(list(col)) as col
    FROM (
        SELECT coalesce({{group_colname}}, '') as group_col, coalesce({{colname}}, '') as col
        FROM {{view_name}}
    )
    GROUP BY group_col
    HAVING count(DISTINCT col) > 1
)
LIMIT 1 ;
