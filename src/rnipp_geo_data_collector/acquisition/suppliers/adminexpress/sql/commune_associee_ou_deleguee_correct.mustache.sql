COPY (
    SELECT
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.fid ELSE t_raw.fid END AS fid,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.cleabs ELSE t_raw.cleabs END AS cleabs,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.nom_officiel ELSE t_raw.nom_officiel END AS nom_officiel,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.nom_officiel_en_majuscules ELSE t_raw.nom_officiel_en_majuscules END AS nom_officiel_en_majuscules,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.code_insee ELSE t_raw.code_insee END AS code_insee,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.code_insee_de_la_commune_de_rattach ELSE t_raw.code_insee_de_la_commune_de_rattach END AS code_insee_de_la_commune_de_rattach,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.code_postal ELSE t_raw.code_postal END AS code_postal,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.code_siren ELSE t_raw.code_siren END AS code_siren,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.nature ELSE t_raw.nature END AS nature,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.population ELSE t_raw.population END AS population,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN ST_GeomFromText(t_add_or_replace.geometrie) ELSE t_raw.geometrie END AS geometrie
    FROM (
        SELECT
            fid,
            cleabs,
            nom_officiel,
            nom_officiel_en_majuscules,
            code_insee,
            code_insee_de_la_commune_de_rattach,
            code_postal,
            code_siren,
            nature,
            population,
            geometrie,
            true as is_present_raw
        FROM {{view_name}}
    ) as t_raw
    FULL JOIN (
        SELECT
            fid,
            cleabs,
            nom_officiel,
            nom_officiel_en_majuscules,
            code_insee,
            code_insee_de_la_commune_de_rattach,
            code_postal,
            code_siren,
            nature,
            population,
            geometrie,
            true as is_present_add_or_replace
        FROM read_csv(
            '{{path_add_or_replace}}',
            delim = ',',
            header = true,
            columns = {
                'fid': 'BIGINT',
                'cleabs': 'VARCHAR',
                'nom_officiel': 'VARCHAR',
                'nom_officiel_en_majuscules': 'VARCHAR',
                'code_insee': 'VARCHAR',
                'code_insee_de_la_commune_de_rattach': 'VARCHAR',
                'code_postal': 'VARCHAR',
                'code_siren': 'VARCHAR',
                'nature': 'VARCHAR',
                'population': 'INTEGER',
                'geometrie': 'VARCHAR'
            }
        )
    ) as t_add_or_replace USING(cleabs)
    FULL JOIN (
        SELECT
            cleabs,
            true as is_remove
        FROM read_csv(
            '{{path_remove}}',
            delim = ',',
            header = true,
            columns = {'cleabs': 'VARCHAR'}
        )
    ) as t_remove USING(cleabs)
    WHERE t_remove.is_remove is NULL OR  (t_remove.is_remove is NOT NULL AND t_add_or_replace.is_present_add_or_replace is NOT NULL)
) TO '{{output_path}}' (FORMAT PARQUET) ;
