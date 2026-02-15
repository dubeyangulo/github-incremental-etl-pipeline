-- Falla si el watermark quedó por delante de lo realmente cargado en FACT (riesgo de "saltarse" datos)
-- Compara por repo (entity_name = full_name)
SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS ok
FROM (
  SELECT
    w.entity_name,
    w.watermark_ts,
    MAX(f.committed_at) AS max_fact
  FROM dw.etl_watermark w
  LEFT JOIN dw.dim_repository d ON d.full_name = w.entity_name
  LEFT JOIN dw.fact_commits f ON f.repo_id = d.repo_id
  WHERE w.pipeline_name = 'github_commits'
  GROUP BY w.entity_name, w.watermark_ts
) t
WHERE t.max_fact IS NULL OR t.max_fact < t.watermark_ts;
