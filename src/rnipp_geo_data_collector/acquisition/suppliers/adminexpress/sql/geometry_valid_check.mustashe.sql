SELECT row_num, key_value
FROM (
    SELECT row_number() OVER () as row_num, coalesce({{key_colname}}, '<NULL>') as key_value, {{geometry_colname}}
    FROM {{view_name}}
)
WHERE {{geometry_colname}} IS NULL OR ST_IsEmpty({{geometry_colname}}) OR NOT ST_IsValid({{geometry_colname}})
LIMIT 1 ;
