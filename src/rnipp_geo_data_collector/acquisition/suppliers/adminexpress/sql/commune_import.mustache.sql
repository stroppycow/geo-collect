CREATE OR REPLACE VIEW {{view_name}} AS (
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
        geometrie
    FROM read_parquet('{{path}}')
) ;
