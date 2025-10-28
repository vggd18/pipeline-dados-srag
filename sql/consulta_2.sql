WITH ContagemComorbidades AS (
  SELECT
    evolucao,
    uti,
    (
      (CASE WHEN cardiopati = true THEN 1 ELSE 0 END) +
      (CASE WHEN diabetes = true THEN 1 ELSE 0 END) +
      (CASE WHEN pneumopati = true THEN 1 ELSE 0 END) +
      (CASE WHEN renal = true THEN 1 ELSE 0 END) +
      (CASE WHEN obesidade = true THEN 1 ELSE 0 END) +
      (CASE WHEN imunodepre = true THEN 1 ELSE 0 END) +
      (CASE WHEN hepatica = true THEN 1 ELSE 0 END) +
      (CASE WHEN neurologic = true THEN 1 ELSE 0 END) +
      (CASE WHEN hematologi = true THEN 1 ELSE 0 END) +
      (CASE WHEN sind_down = true THEN 1 ELSE 0 END)
    ) AS total_comorbidades
  FROM srag
  WHERE classi_fin = 'SRAG por covid-19'
)

SELECT
  CASE
    WHEN total_comorbidades >= 3 THEN '3+'
    ELSE CAST(total_comorbidades AS VARCHAR)
  END AS n_comorbidades,
  COUNT(*) AS total_casos,
  AVG(CASE WHEN uti = true THEN 1.0 ELSE 0.0 END) AS taxa_uti,
  AVG(CASE WHEN evolucao = 'Óbito' THEN 1.0 ELSE 0.0 END) AS taxa_letalidade
FROM ContagemComorbidades
GROUP BY n_comorbidades
ORDER BY n_comorbidades;