SELECT uri, duplicate_rows
FROM (
    SELECT uri, string_agg(CAST(row_num AS VARCHAR), ', ') as duplicate_rows
    FROM (
        SELECT row_number() OVER () as row_num, uri
        FROM {{view_name}}
    ) as t1
    GROUP BY uri
    HAVING count(*) > 1
)
LIMIT 1 ;
