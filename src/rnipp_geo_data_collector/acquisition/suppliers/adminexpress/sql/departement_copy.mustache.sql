COPY (
    SELECT
        fid,
        cleabs,
        nom_officiel,
        nom_officiel_en_majuscules,
        code_insee,
        code_insee_de_la_region,
        code_siren,
        geometrie
    FROM read_parquet('{{input_path}}')
) TO '{{output_path}}' (FORMAT PARQUET) ;
