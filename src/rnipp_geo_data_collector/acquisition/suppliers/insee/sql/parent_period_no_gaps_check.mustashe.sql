SELECT
    coalesce(t_before.row_num, t_after.row_num) as row_num,
    t_before.uri as uri,
    t_before.parent_uri as parent_uri
FROM (
    SELECT row_num, uri, coalesce(t_child_a.parent_uri, t_parent_a.parent_uri) as parent_uri, start_date, end_date
    FROM (
        SELECT row_num, uri, regexp_split_to_table(parent_uri, '[|]') as parent_uri
        FROM (
            SELECT row_number() OVER () as row_num, uri, parent_uri
            FROM {{view_name_child}}
        ) as t1_a
    ) as t_child_a
    FULL JOIN (
        SELECT parent_uri, start_date, coalesce(end_date, date_add(today(), INTERVAL 1 DAY)) as end_date
        FROM (
            {{sql_import_parent}}
        ) as t2_a
    ) as t_parent_a ON t_child_a.parent_uri = t_parent_a.parent_uri
    WHERE coalesce(uri, '') <> '' and t_parent_a.start_date <> t_parent_a.end_date
) as t_before
LEFT JOIN (
    SELECT row_num, uri, coalesce(t_child_b.parent_uri, t_parent_b.parent_uri) as parent_uri, start_date, end_date, true as present_after
    FROM (
        SELECT row_num, uri, regexp_split_to_table(parent_uri, '[|]') as parent_uri
        FROM (
            SELECT row_number() OVER () as row_num, uri, parent_uri
            FROM {{view_name_child}}
        ) as t1_b
    ) as t_child_b
    FULL JOIN (
        SELECT parent_uri, start_date, coalesce(end_date, date_add(today(), INTERVAL 1 DAY)) as end_date
        FROM (
            {{sql_import_parent}}
        ) as t2_b
    ) as t_parent_b ON t_child_b.parent_uri = t_parent_b.parent_uri
    WHERE coalesce(uri, '') <> ''
) as t_after
    ON t_before.uri = t_after.uri
    AND t_before.parent_uri <> t_after.parent_uri
    AND t_before.end_date = t_after.start_date
LEFT JOIN (
    SELECT uri, max(end_date) as max_end_date
    FROM (
        SELECT uri, regexp_split_to_table(parent_uri, '[|]') as parent_uri
        FROM (
            SELECT uri, parent_uri
            FROM {{view_name_child}}
        ) as t1_c
    ) as t_child_c
    FULL JOIN (
        SELECT parent_uri, coalesce(end_date, date_add(today(), INTERVAL 1 DAY)) as end_date
        FROM (
            {{sql_import_parent}}
        ) as t2_c
    ) as t_parent_c ON t_child_c.parent_uri = t_parent_c.parent_uri
    WHERE coalesce(uri, '') <> ''
    GROUP BY uri
) as t_max ON t_before.uri = t_max.uri
WHERE present_after is NULL  AND coalesce(t_before.end_date, date_add(today(), INTERVAL 1 DAY)) < coalesce(t_max.max_end_date, date_add(today(), INTERVAL 1 DAY))
LIMIT 1 ;
