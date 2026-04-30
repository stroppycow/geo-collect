SELECT insee_code, postal_code, associated_names
FROM (
    SELECT insee_code, postal_code, sum(CASE WHEN associated_name = '' THEN 1 ELSE 0 END) < 2 as test, list(associated_name) as associated_names
    FROM (
        SELECT coalesce(insee_code, '') as insee_code, coalesce(postal_code, '') as postal_code, coalesce(associated_name, '') as associated_name
        FROM {{view_name}}
    )
    GROUP BY insee_code, postal_code
    HAVING count(*) > 1
)
WHERE not(test)
LIMIT 1 ;
