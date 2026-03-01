SELECT row_num, uri, end_event_uri, end_date
FROM (
    SELECT
        row_number() OVER () as row_num,
        uri,
        end_event_uri,
        end_date
    FROM {{view_name}}
)
WHERE (end_event_uri is null and end_date is not null) OR (end_event_uri is not null and end_date is null)
LIMIT 1 ;
