SELECT
  CASE
    WHEN nu_idade_n < 17 AND tp_idade = '3' THEN 'pediatrico'
    WHEN tp_idade = '1' OR tp_idade = '2' THEN 'pediatrico'
    WHEN nu_idade_n >= 17 AND nu_idade_n < 60 AND tp_idade = '3' THEN 'adulto'
    ELSE 'idoso'
  END AS grupo_risco,
  COUNT(*) AS quantidade,
  AVG(CASE WHEN uti = true THEN 1.0 ELSE 0.0 END) AS taxa_uti,
  AVG(CASE WHEN evolucao = 'Óbito' THEN 1.0 ELSE 0.0 END) AS taxa_obito
FROM srag
WHERE classi_fin = 'SRAG por covid-19' AND co_mun_res = '261160'
GROUP BY grupo_risco;