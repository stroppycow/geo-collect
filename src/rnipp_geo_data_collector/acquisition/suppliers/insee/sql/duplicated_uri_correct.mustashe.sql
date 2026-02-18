COPY (
    SELECT * EXCLUDE (rn, is_duplicated, rowid)
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY var_1 ORDER BY rowid) AS rn
        FROM (
            SELECT *, ROW_NUMBER() OVER () as  rowid, {{is_duplicated_sql}} as is_duplicated
            FROM {{view_name}}
        )
    )
    WHERE (rn = 1 AND is_duplicated) OR (NOT is_duplicated))
        
) TO '{{output_path}}' (FORMAT CSV, HEADER TRUE) ;
