CREATE OR REPLACE VIEW {{view_name}} AS (
    SELECT
        insee_code,
        name,
        postal_code,
        delivery_label,
        associated_name
    FROM read_csv(
        '{{path}}',
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
) ;
