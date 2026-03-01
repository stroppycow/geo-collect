SELECT event_uri, array_to_string(list_distinct(list(event_date)), ',') as events_dates
FROM (
    {{sql_events_extract}}
)
GROUP BY event_uri
HAVING count(DISTINCT event_date) > 1
LIMIT 1 ;
