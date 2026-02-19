SELECT *
FROM (
    SELECT
        row_number() OVER () as row_num,
        uri,
        coalesce({{parent_field}}, '') as parent_field,
        coalesce({{parent_field_count}}, -1::INTEGER) as parent_field_count,
        CASE
            WHEN (coalesce({{parent_field}}, '') = '') OR (coalesce({{parent_field_count}}, -1::INTEGER) = 0) THEN 'EMPTY_PARENT_FIELD'
            WHEN coalesce({{parent_field_count}}, -1::INTEGER) > 1 THEN 'MULTIPLE_PARENTS'
            ELSE 'UNIQUE_PARENT'
        END as parent_unicity_status
    FROM {{view_name}}
)
WHERE parent_unicity_status != 'UNIQUE_PARENT'
LIMIT 1 ;
