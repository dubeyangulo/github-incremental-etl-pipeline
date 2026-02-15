-- Watermark = máximo committed_at ya cargado a FACT por repo
INSERT INTO dw.etl_watermark (pipeline_name, entity_name, watermark_ts)
SELECT
  'github_commits' AS pipeline_name,
  d.full_name      AS entity_name,
  MAX(f.committed_at) AS watermark_ts
FROM dw.fact_commits f
JOIN dw.dim_repository d ON d.repo_id = f.repo_id
GROUP BY d.full_name
ON CONFLICT (pipeline_name, entity_name)
DO UPDATE SET watermark_ts = EXCLUDED.watermark_ts, updated_at = NOW();
