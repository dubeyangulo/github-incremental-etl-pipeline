-- Toma la última extracción en raw.github_repos (por org) y la sube a stg.repos

WITH last_batch AS (
  SELECT org_or_user, payload, ingested_at
  FROM raw.github_repos
  WHERE org_or_user = '{{ var.value.GITHUB_ORG | default("apache") }}'
  ORDER BY ingested_at DESC
  LIMIT 1
),
repos AS (
  SELECT
    (r->>'id')::bigint                AS repo_id,
    r->>'full_name'                   AS full_name,
    r->>'name'                        AS name,
    (r->'owner'->>'login')            AS owner_login,
    COALESCE((r->>'private')::boolean, false) AS is_private,
    r->>'html_url'                    AS html_url,
    r->>'description'                 AS description,
    r->>'language'                    AS language,
    r->>'default_branch'              AS default_branch,
    (r->>'created_at')::timestamptz   AS created_at,
    (r->>'updated_at')::timestamptz   AS updated_at,
    (r->>'pushed_at')::timestamptz    AS pushed_at,
    (r->>'stargazers_count')::int     AS stargazers_count,
    (r->>'forks_count')::int          AS forks_count,
    (r->>'open_issues_count')::int    AS open_issues_count,
    lb.ingested_at                    AS raw_ingested_at
  FROM last_batch lb
  CROSS JOIN LATERAL jsonb_array_elements(lb.payload) r
  WHERE (r->>'id') IS NOT NULL
)
INSERT INTO stg.repos (
  repo_id, full_name, name, owner_login, is_private, html_url, description,
  language, default_branch, created_at, updated_at, pushed_at,
  stargazers_count, forks_count, open_issues_count, raw_ingested_at, last_seen_at
)
SELECT
  repo_id, full_name, name, owner_login, is_private, html_url, description,
  language, default_branch, created_at, updated_at, pushed_at,
  stargazers_count, forks_count, open_issues_count, raw_ingested_at, NOW()
FROM repos
ON CONFLICT (repo_id) DO UPDATE SET
  full_name = EXCLUDED.full_name,
  name = EXCLUDED.name,
  owner_login = EXCLUDED.owner_login,
  is_private = EXCLUDED.is_private,
  html_url = EXCLUDED.html_url,
  description = EXCLUDED.description,
  language = EXCLUDED.language,
  default_branch = EXCLUDED.default_branch,
  created_at = EXCLUDED.created_at,
  updated_at = EXCLUDED.updated_at,
  pushed_at = EXCLUDED.pushed_at,
  stargazers_count = EXCLUDED.stargazers_count,
  forks_count = EXCLUDED.forks_count,
  open_issues_count = EXCLUDED.open_issues_count,
  raw_ingested_at = EXCLUDED.raw_ingested_at,
  last_seen_at = NOW();
