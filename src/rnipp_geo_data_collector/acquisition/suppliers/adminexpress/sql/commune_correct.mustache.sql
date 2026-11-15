COPY (
    SELECT
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.fid ELSE t_raw.fid END AS fid,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.cleabs ELSE t_raw.cleabs END AS cleabs,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.nom_officiel ELSE t_raw.nom_officiel END AS nom_officiel,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.nom_officiel_en_majuscules ELSE t_raw.nom_officiel_en_majuscules END AS nom_officiel_en_majuscules,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.statut ELSE t_raw.statut END AS statut,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.code_insee ELSE t_raw.code_insee END AS code_insee,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.population ELSE t_raw.population END AS population,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.date_du_recensement ELSE t_raw.date_du_recensement END AS date_du_recensement,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.organisme_recenseur ELSE t_raw.organisme_recenseur END AS organisme_recenseur,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.code_insee_du_canton ELSE t_raw.code_insee_du_canton END AS code_insee_du_canton,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.code_insee_de_l_arrondissement ELSE t_raw.code_insee_de_l_arrondissement END AS code_insee_de_l_arrondissement,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.code_insee_du_departement ELSE t_raw.code_insee_du_departement END AS code_insee_du_departement,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.code_insee_de_la_region ELSE t_raw.code_insee_de_la_region END AS code_insee_de_la_region,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.code_siren ELSE t_raw.code_siren END AS code_siren,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.codes_siren_des_epci ELSE t_raw.codes_siren_des_epci END AS codes_siren_des_epci,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.code_postal ELSE t_raw.code_postal END AS code_postal,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN t_add_or_replace.superficie_cadastrale ELSE t_raw.superficie_cadastrale END AS superficie_cadastrale,
        CASE WHEN t_add_or_replace.is_present_add_or_replace is not null THEN ST_GeomFromText(t_add_or_replace.geometrie) ELSE t_raw.geometrie END AS geometrie
    FROM (
        SELECT
            fid,
            cleabs,
            nom_officiel,
            nom_officiel_en_majuscules,
            statut,
            code_insee,
            population,
            date_du_recensement,
            organisme_recenseur,
            code_insee_du_canton,
            code_insee_de_l_arrondissement,
            code_insee_du_departement,
            code_insee_de_la_region,
            code_siren,
            codes_siren_des_epci,
            code_postal,
            superficie_cadastrale,
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
            statut,
            code_insee,
            population,
            date_du_recensement,
            organisme_recenseur,
            code_insee_du_canton,
            code_insee_de_l_arrondissement,
            code_insee_du_departement,
            code_insee_de_la_region,
            code_siren,
            codes_siren_des_epci,
            code_postal,
            superficie_cadastrale,
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
                'statut': 'VARCHAR',
                'code_insee': 'VARCHAR',
                'population': 'INTEGER',
                'date_du_recensement': 'DATE',
                'organisme_recenseur': 'VARCHAR',
                'code_insee_du_canton': 'VARCHAR',
                'code_insee_de_l_arrondissement': 'VARCHAR',
                'code_insee_du_departement': 'VARCHAR',
                'code_insee_de_la_region': 'VARCHAR',
                'code_siren': 'VARCHAR',
                'codes_siren_des_epci': 'VARCHAR',
                'code_postal': 'VARCHAR',
                'superficie_cadastrale': 'INTEGER',
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
