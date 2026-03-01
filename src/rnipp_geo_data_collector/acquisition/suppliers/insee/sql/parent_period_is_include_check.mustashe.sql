SELECT row_num, uri, start_date, end_date, start_date_parent_min, end_date_parent_max
FROM (
    SELECT
        any_value(row_num) as row_num,
        uri,
        any_value(start_date) as start_date,
        any_value(end_date) as end_date,
        min(start_date_parent) as start_date_parent_min,
        CASE
            WHEN bool_or(end_date_parent is NULL)
            THEN NULL::DATE
            ELSE max(end_date_parent)
        END as end_date_parent_max
    FROM (
        SELECT row_num, uri, regexp_split_to_table(parent_uri, '[|]') as parent_uri, start_date, end_date
        FROM (
            SELECT row_number() OVER () as row_num, uri, parent_uri, start_date, end_date
            FROM {{view_name_child}}
        ) as t1
    ) as t_child
    LEFT JOIN (
        SELECT parent_uri, start_date as start_date_parent, end_date as end_date_parent
        FROM (
            {{sql_import_parent}}
        ) as t2
    ) as t_parent ON t_child.parent_uri = t_parent.parent_uri
    GROUP BY uri
) as t_main
WHERE (start_date < start_date_parent_min) OR (coalesce(end_date, today()) > coalesce(end_date_parent_max, today()))
LIMIT 1 ;
