SELECT row_num, uri, start_date, end_date
FROM (
    SELECT
        row_number() OVER () as row_num,
        uri,
        start_date,
        end_date
    FROM {{view_name}}

)
WHERE end_date is not null and start_date > coalesce(end_date, today())
LIMIT 1 ;
