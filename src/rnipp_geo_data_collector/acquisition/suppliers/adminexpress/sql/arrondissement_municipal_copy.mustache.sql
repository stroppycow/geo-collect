COPY (
    SELECT
        fid,
        cleabs,
        nom_officiel,
        nom_officiel_en_majuscules,
        numero_de_l_arrondissement_municipal,
        code_insee,
        code_insee_de_la_commune_de_rattach,
        code_postal,
        population,
        geometrie
    FROM read_parquet('{{input_path}}')
) TO '{{output_path}}' (FORMAT PARQUET) ;
