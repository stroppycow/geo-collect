SELECT
    a.insee_code as insee_code,
    a.uri as uri_a,
    b.uri as uri_b
    FROM (
        SELECT uri, insee_code, start_date, coalesce(end_date, date_add(today(), INTERVAL 1 DAY)) as end_date
        FROM {{view_name}}
    ) as a
    JOIN (
        SELECT uri, insee_code, start_date, coalesce(end_date, date_add(today(), INTERVAL 1 DAY)) as end_date
        FROM {{view_name}}
    ) as b
      ON a.insee_code = b.insee_code
     AND a.uri <> b.uri
     AND a.start_date < b.end_date
     AND b.start_date < a.end_date
LIMIT 1 ;
