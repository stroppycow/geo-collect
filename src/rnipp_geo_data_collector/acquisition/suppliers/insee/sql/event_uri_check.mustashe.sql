SELECT row_number() OVER () as row_num, uri, coalesce({{event_uri}}, '') as {{event_uri}}
FROM {{view_name}}
WHERE not(regexp_matches(coalesce({{event_uri}}, ''), '{{pattern}}'))
LIMIT 1 ;
