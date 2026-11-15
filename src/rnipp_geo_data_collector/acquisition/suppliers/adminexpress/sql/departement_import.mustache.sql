CREATE OR REPLACE VIEW {{view_name}} AS (
    SELECT
        fid,
        cleabs,
        nom_officiel,
        nom_officiel_en_majuscules,
        code_insee,
        code_insee_de_la_region,
        code_siren,
        geometrie
    FROM read_parquet('{{path}}')
) ;
