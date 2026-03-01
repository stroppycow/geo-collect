SELECT row_num, uri, coalesce(t_child.parent_uri, t_parent.parent_uri) as parent_uri
FROM (
    SELECT row_num, uri, regexp_split_to_table(parent_uri, '[|]') as parent_uri
    FROM (
        SELECT row_number() OVER () as row_num, uri, parent_uri
        FROM {{view_name_child}}
    ) as t1
) as t_child
LEFT JOIN (
    SELECT parent_uri, true as parent_exist
    FROM (
        {{sql_import_parent}}
    ) as t2
) as t_parent ON t_child.parent_uri = t_parent.parent_uri
WHERE not(coalesce(t_parent.parent_exist, false))
LIMIT 1 ;
