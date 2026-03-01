COPY (
    SELECT 
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.uri ELSE t_raw.uri END AS uri,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.insee_code ELSE t_raw.insee_code END AS insee_code,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.label ELSE t_raw.label END AS label,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.article_code ELSE t_raw.article_code END AS article_code,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.parent_uri ELSE t_raw.parent_uri END AS parent_uri,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.start_event_uri ELSE t_raw.start_event_uri END AS start_event_uri,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.end_event_uri ELSE t_raw.end_event_uri END AS end_event_uri,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.start_date ELSE t_raw.start_date END AS start_date,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.end_date ELSE t_raw.end_date END AS end_date,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.parent_uri_count ELSE t_raw.parent_uri_count END AS parent_uri_count,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.start_date_count ELSE t_raw.start_date_count END AS start_date_count,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.end_date_count ELSE t_raw.end_date_count END AS end_date_count
    FROM (
        SELECT 
            uri,
            insee_code,
            label,
            article_code,
            parent_uri,
            start_event_uri,
            end_event_uri,
            start_date,
            end_date,
            parent_uri_count,
            start_date_count,
            end_date_count,
            true as is_present_raw
        FROM {{view_name}}
    ) as t_raw
    FULL JOIN (
        SELECT
            uri,
            insee_code,
            label,
            article_code,
            parent_uri,
            start_event_uri,
            end_event_uri,
            start_date,
            end_date,
            parent_uri_count,
            start_date_count,
            end_date_count,
            true as is_present_add_or_replace
        FROM read_csv(
            '{{path_add_or_replace}}',
            delim = ',',
            header = true,
            columns = {
                'uri': 'VARCHAR',
                'insee_code': 'VARCHAR',
                'label': 'VARCHAR',
                'article_code': 'VARCHAR',
                'parent_uri': 'VARCHAR',
                'start_event_uri': 'VARCHAR',
                'end_event_uri': 'VARCHAR',
                'start_date': 'DATE',
                'end_date': 'DATE',
                'parent_uri_count': 'INTEGER',
                'start_date_count': 'INTEGER',
                'end_date_count': 'INTEGER'
            }
        )
    ) as t_add_or_replace USING(uri)
    FULL JOIN (
        SELECT
            uri,
            true as is_remove
        FROM read_csv(
            '{{path_remove}}',
            delim = ',',
            header = true,
            columns = {'uri': 'VARCHAR'}
        )
    ) as t_remove USING(uri)
    WHERE t_remove.is_remove is NULL OR  (t_remove.is_remove is NOT NULL AND t_add_or_replace.is_present_add_or_replace is NOT NULL)
) TO '{{output_path}}' (FORMAT CSV, HEADER TRUE) ;
