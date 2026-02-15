-- ============================
-- 0) Schemas
-- ============================
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dw;

-- ============================
-- 1) RAW (JSON crudo)
-- ============================
CREATE TABLE IF NOT EXISTS raw.github_repos (
  ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  source           TEXT NOT NULL DEFAULT 'github_api',
  org_or_user      TEXT NOT NULL,
  payload          JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_raw_github_repos_ingested_at
  ON raw.github_repos (ingested_at);

CREATE TABLE IF NOT EXISTS raw.github_commits (
  ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  source           TEXT NOT NULL DEFAULT 'github_api',
  org_or_user      TEXT NOT NULL,
  repo_full_name   TEXT NOT NULL,               -- ej: "apache/airflow"
  payload          JSONB NOT NULL                -- cada fila: un commit (JSON)
);

CREATE INDEX IF NOT EXISTS ix_raw_github_commits_ingested_at
  ON raw.github_commits (ingested_at);

CREATE INDEX IF NOT EXISTS ix_raw_github_commits_repo
  ON raw.github_commits (repo_full_name);

-- ============================
-- 2) STAGING (aplanado)
-- ============================
CREATE TABLE IF NOT EXISTS stg.repos (
  repo_id          BIGINT PRIMARY KEY,
  full_name        TEXT NOT NULL,
  name             TEXT NOT NULL,
  owner_login      TEXT NOT NULL,
  is_private       BOOLEAN NOT NULL,
  html_url         TEXT,
  description      TEXT,
  language         TEXT,
  default_branch   TEXT,
  created_at       TIMESTAMPTZ,
  updated_at       TIMESTAMPTZ,
  pushed_at        TIMESTAMPTZ,
  stargazers_count INT,
  forks_count      INT,
  open_issues_count INT,
  raw_ingested_at  TIMESTAMPTZ NOT NULL,
  last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_stg_repos_owner
  ON stg.repos (owner_login);

CREATE INDEX IF NOT EXISTS ix_stg_repos_full_name
  ON stg.repos (full_name);

CREATE TABLE IF NOT EXISTS stg.commits (
  sha              TEXT PRIMARY KEY,
  repo_full_name   TEXT NOT NULL,
  repo_id          BIGINT,                       -- se puede llenar por join con stg.repos
  author_name      TEXT,
  author_email     TEXT,
  author_login     TEXT,
  commit_message   TEXT,
  committed_at     TIMESTAMPTZ,
  additions        INT,
  deletions        INT,
  changed_files    INT,
  raw_ingested_at  TIMESTAMPTZ NOT NULL,
  last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_stg_commits_repo
  ON stg.commits (repo_full_name);

CREATE INDEX IF NOT EXISTS ix_stg_commits_committed_at
  ON stg.commits (committed_at);

-- ============================
-- 3) DATA WAREHOUSE (dim/fact)
-- ============================
CREATE TABLE IF NOT EXISTS dw.dim_repository (
  repo_key         BIGSERIAL PRIMARY KEY,
  repo_id          BIGINT UNIQUE NOT NULL,
  full_name        TEXT NOT NULL,
  name             TEXT NOT NULL,
  owner_login      TEXT NOT NULL,
  is_private       BOOLEAN NOT NULL,
  html_url         TEXT,
  description      TEXT,
  language         TEXT,
  default_branch   TEXT,
  created_at       TIMESTAMPTZ,
  updated_at       TIMESTAMPTZ,
  pushed_at        TIMESTAMPTZ,
  stargazers_count INT,
  forks_count      INT,
  open_issues_count INT,
  effective_from   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  effective_to     TIMESTAMPTZ,
  is_current       BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS ix_dim_repository_full_name
  ON dw.dim_repository (full_name);

CREATE TABLE IF NOT EXISTS dw.fact_commits (
  commit_key       BIGSERIAL PRIMARY KEY,
  sha              TEXT UNIQUE NOT NULL,
  repo_id          BIGINT NOT NULL,
  repo_key         BIGINT,                       -- opcional (si quieres surrogate key)
  committed_at     TIMESTAMPTZ,
  author_login     TEXT,
  author_name      TEXT,
  author_email     TEXT,
  commit_message   TEXT,
  additions        INT,
  deletions        INT,
  changed_files    INT,
  load_dts         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_fact_commits_repo_id
  ON dw.fact_commits (repo_id);

CREATE INDEX IF NOT EXISTS ix_fact_commits_committed_at
  ON dw.fact_commits (committed_at);
