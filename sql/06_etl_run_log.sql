CREATE TABLE IF NOT EXISTS dw.etl_run_log (
  pipeline_name        TEXT NOT NULL,
  dag_id               TEXT NOT NULL,
  run_id               TEXT NOT NULL,
  logical_date         TIMESTAMPTZ,
  start_ts             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  end_ts               TIMESTAMPTZ,
  status               TEXT NOT NULL DEFAULT 'RUNNING', -- RUNNING / SUCCESS / FAILED
  error_message        TEXT,

  -- métricas (puedes ir ampliando)
  raw_repos_batches     BIGINT,
  raw_commits_batches   BIGINT,
  stg_repos_rows        BIGINT,
  stg_commits_rows      BIGINT,
  dim_repository_rows   BIGINT,
  fact_commits_rows     BIGINT,

  PRIMARY KEY (pipeline_name, dag_id, run_id)
);

CREATE INDEX IF NOT EXISTS ix_etl_run_log_start_ts
  ON dw.etl_run_log (start_ts DESC);
