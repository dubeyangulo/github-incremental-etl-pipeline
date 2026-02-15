CREATE TABLE IF NOT EXISTS dw.etl_watermark (
  pipeline_name   TEXT NOT NULL,
  entity_name     TEXT NOT NULL,          -- ej: repo_full_name o "org"
  watermark_ts    TIMESTAMPTZ,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (pipeline_name, entity_name)
);

-- Valor inicial opcional (si no existe, el DAG usará default)
-- Puedes dejarlo vacío y manejarlo desde código.
