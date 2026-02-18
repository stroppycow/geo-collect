SELECT row_number() OVER () as row_num, uri, coalesce(insee_code, '') as insee_code
FROM {{view_name}}
WHERE not(regexp_matches(coalesce(insee_code, ''), '{{pattern}}'))
LIMIT 1 ;
