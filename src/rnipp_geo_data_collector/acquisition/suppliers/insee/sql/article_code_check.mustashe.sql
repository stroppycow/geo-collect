SELECT row_number() OVER () as row_num, uri, coalesce(article_code, '') as article_code
FROM {{view_name}}
WHERE not(regexp_matches(coalesce(article_code, ''), '{{pattern}}'))
LIMIT 1 ;
