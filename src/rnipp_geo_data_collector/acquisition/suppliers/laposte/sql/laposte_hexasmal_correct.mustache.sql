COPY (
    SELECT 
        CASE WHEN t_add.is_present_add is not null THEN t_add.insee_code ELSE t_raw.insee_code END AS insee_code,
        CASE WHEN t_add.is_present_add is not null THEN t_add.name ELSE t_raw.name END AS name,
        CASE WHEN t_add.is_present_add is not null THEN t_add.postal_code ELSE t_raw.postal_code END AS postal_code,
        CASE WHEN t_add.is_present_add is not null THEN t_add.delivery_label ELSE t_raw.delivery_label END AS delivery_label,
        CASE WHEN t_add.is_present_add is not null THEN t_add.associated_name ELSE t_raw.associated_name END AS associated_name
    FROM (
        SELECT 
            insee_code,
            name,
            postal_code,
            delivery_label,
            associated_name,
            true as is_present_raw
        FROM {{view_name}}
    ) as t_raw
    FULL JOIN (
        SELECT
            insee_code,
            name,
            postal_code,
            delivery_label,
            associated_name,
            true as is_present_add
        FROM read_csv(
            '{{path_add}}',
            delim = ',',
            header = true,
            columns = {
                'insee_code': 'VARCHAR',
                'name': 'VARCHAR',
                'postal_code': 'VARCHAR',
                'delivery_label': 'VARCHAR',
                'associated_name': 'VARCHAR'
            }
        )
    ) as t_add USING(insee_code)
    FULL JOIN (
        SELECT
            insee_code,
            true as is_remove
        FROM read_csv(
            '{{path_remove}}',
            delim = ',',
            header = true,
            columns = {'insee_code': 'VARCHAR'}
        )
    ) as t_remove USING(insee_code)
    WHERE t_remove.is_remove is NULL OR  (t_remove.is_remove is NOT NULL AND t_add.is_present_add is NOT NULL)
) TO '{{output_path}}' (FORMAT CSV, HEADER TRUE) ;
