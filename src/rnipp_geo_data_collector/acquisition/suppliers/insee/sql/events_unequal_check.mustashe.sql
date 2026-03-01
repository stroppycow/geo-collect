SELECT row_num, uri, coalesce(start_event_uri, '') as start_event_uri, coalesce(end_event_uri, '') as end_event_uri
FROM (
    SELECT
        row_number() OVER () as row_num,
        uri,
        start_event_uri,
        end_event_uri
    FROM {{view_name}}
)
WHERE coalesce(start_event_uri, '') == coalesce(end_event_uri, '')
LIMIT 1 ;
