SELECT row_number() OVER () as row_num, coalesce(uri, '') as uri
FROM {{view_name}}
WHERE not(regexp_matches(coalesce(uri, ''), '{{pattern}}'))
LIMIT 1 ;
