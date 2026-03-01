SELECT
    coalesce(a.row_num, b.row_num) as row_num,
    coalesce(a.uri, b.uri) as uri,
    a.parent_uri as parent_uri_a,
    b.parent_uri as parent_uri_b
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
    WHERE coalesce(uri, '') <> ''
) as a
JOIN (
    SELECT row_num, uri, coalesce(t_child_b.parent_uri, t_parent_b.parent_uri) as parent_uri, start_date, end_date
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
) as b
    ON a.uri = b.uri
    AND a.parent_uri <> b.parent_uri
    AND a.start_date < b.end_date
    AND b.start_date < a.end_date
LIMIT 1 ;
