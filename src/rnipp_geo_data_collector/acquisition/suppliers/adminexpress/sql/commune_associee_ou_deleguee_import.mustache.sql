CREATE OR REPLACE VIEW {{view_name}} AS (
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
        geometrie
    FROM read_parquet('{{path}}')
) ;
