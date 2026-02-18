SELECT row_num, uri, start_date, start_date_count, date_category
FROM (
    SELECT
        row_number() OVER () as row_num,
        uri,
        start_date,
        start_date_count,
        CASE
            WHEN start_date IS NULL THEN 'NULL'
            WHEN start_date < '1900-01-01'::DATE THEN 'BEFORE'
            WHEN start_date > today() THEN 'AFTER'
            WHEN start_date_count > 1 THEN 'MULTIPLE'
            ELSE CAST(NULL AS VARCHAR)
        END AS date_category
    FROM {{view_name}}
)
WHERE date_category IS NOT NULL
LIMIT 1 ;
