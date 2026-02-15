from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.state import State
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from airflow.settings import Session
from airflow.models.taskinstance import TaskInstance
import json
import requests



DAG_ID = "github_etl"
POSTGRES_CONN_ID = "airflow_postgres"

default_args = {
    "owner": "dubey",
    "retries": 2,
}


with DAG(
    dag_id=DAG_ID,
    description="ETL GitHub API -> Raw -> Staging -> DW (incremental)",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "github", "postgres"],
    template_searchpath=["/opt/airflow/sql"], 
    
) as dag:

    init_schema = PostgresOperator(
        task_id="init_schema",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="00_init_schema.sql",
        autocommit=True,
    )

    create_control_tables = PostgresOperator(
        task_id="create_control_tables",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="05_control_tables.sql",
        autocommit=True,
    )

    create_run_log_table = PostgresOperator(
        task_id="create_run_log_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="06_etl_run_log.sql",
        autocommit=True,
    )


    transform_repos = PostgresOperator(
        task_id="transform_repos_to_stg",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="20_transform_repos_to_stg.sql",
        autocommit=True,
    )


    @task(task_id="extract_repos_to_raw")
    def extract_repos_to_raw():
        org = Variable.get("GITHUB_ORG", default_var="apache")
        token = Variable.get("GITHUB_TOKEN", default_var=None)

        # Import desde /opt/airflow/src
        import sys
        sys.path.append("/opt/airflow/src")
        from extract.github_client import GitHubClient

        client = GitHubClient(token=token)
        repos = client.list_org_repos(org=org, per_page=100, max_pages=10)

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            INSERT INTO raw.github_repos (org_or_user, payload)
            VALUES (%s, %s::jsonb)
        """
        # Guardamos el payload completo como un JSON array para esa extracción
        payload_json = json.dumps(repos)

        hook.run(sql, parameters=(org, payload_json))

        return {"org": org, "count": len(repos)}
    
    extract_raw = extract_repos_to_raw()


    #Datawerehouse
    load_dim_repository = PostgresOperator(
        task_id="load_dim_repository",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="30_load_dim_repository.sql",
        autocommit=True,
    )

    @task(task_id="extract_commits_to_raw")
    def extract_commits_to_raw():
        org = Variable.get("GITHUB_ORG", default_var="apache")
        token = Variable.get("GITHUB_TOKEN", default_var=None)

        headers = {"User-Agent": "etl-github-pipeline"}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Ajusta el límite cuando todo esté estable
        repos = hook.get_records("SELECT full_name FROM stg.repos ORDER BY pushed_at DESC NULLS LAST LIMIT 10;")

        pipeline_name = "github_commits"

        # default watermark si no existe: últimos 7 días
        default_since = (datetime.now(timezone.utc).replace(microsecond=0))
        # restar 7 días sin usar timedelta para evitar imports extra (pero mejor importarlo)
        from datetime import timedelta
        default_since = default_since - timedelta(days=7)

        total_commits = 0
        updated = 0

        for (repo_full_name,) in repos:
            # 1) Leer watermark actual
            row = hook.get_first(
                """
                SELECT watermark_ts
                FROM dw.etl_watermark
                WHERE pipeline_name = %s AND entity_name = %s
                """,
                parameters=(pipeline_name, repo_full_name),
            )
            since_ts = row[0] if row and row[0] else default_since

            # 2) Extraer commits desde GitHub usando since
            commits_all = []
            per_page = 100
            max_pages = 10  # evita infinito; subimos luego si quieres

            for page in range(1, max_pages + 1):
                url = f"https://api.github.com/repos/{repo_full_name}/commits"
                resp = requests.get(
                    url,
                    headers=headers,
                    params={
                        "per_page": per_page,
                        "page": page,
                        "since": since_ts.isoformat().replace("+00:00", "Z"),
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                batch = resp.json()
                if not batch:
                    break
                commits_all.extend(batch)
                if len(batch) < per_page:
                    break

            # 3) Insertar RAW (guardamos el array completo del repo)
            hook.run(
                """
                INSERT INTO raw.github_commits (org_or_user, repo_full_name, payload)
                VALUES (%s, %s, %s::jsonb)
                """,
                parameters=(org, repo_full_name, json.dumps(commits_all)),
            )

            total_commits += len(commits_all)

            # 4) Calcular nuevo watermark (max committed_at del payload)
            # GitHub: commit.author.date viene como ISO string
            max_dt = None
            for c in commits_all:
                try:
                    dt_str = c["commit"]["author"]["date"]
                    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
                    if (max_dt is None) or (dt > max_dt):
                        max_dt = dt
                except Exception:
                    continue

            # 5) Actualizar watermark SOLO si hubo commits nuevos con fecha válida
            

        return {"repos": len(repos), "total_commits": total_commits, "watermarks_updated": updated}

    
    transform_commits = PostgresOperator(
        task_id="transform_commits_to_stg",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="40_transform_commits_to_stg.sql",
        autocommit=True,
    )

    load_fact_commits = PostgresOperator(
        task_id="load_fact_commits",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="50_load_fact_commits.sql",
        autocommit=True,
    )

    update_watermark = PostgresOperator(
        task_id="update_watermark_after_load",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="60_update_watermark_after_load.sql",
        autocommit=True,
    )

    PIPELINE_NAME = "github_etl"

    @task(task_id="log_run_start")
    def log_run_start(**context):
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        dag_id = context["dag"].dag_id
        run_id = context["run_id"]
        logical_date = context.get("logical_date")  # Airflow 2.x

        hook.run(
            """
            INSERT INTO dw.etl_run_log (pipeline_name, dag_id, run_id, logical_date, status, start_ts)
            VALUES (%s, %s, %s, %s, 'RUNNING', NOW())
            ON CONFLICT (pipeline_name, dag_id, run_id) DO UPDATE
            SET status='RUNNING', start_ts=NOW(), end_ts=NULL, error_message=NULL;
            """,
            parameters=(PIPELINE_NAME, dag_id, run_id, logical_date),
        )

        return {"pipeline": PIPELINE_NAME, "dag_id": dag_id, "run_id": run_id}

    @task(task_id="log_run_end", trigger_rule="all_done")
    def log_run_end(start_info: dict, **context):
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        dag_id = start_info["dag_id"]
        run_id = start_info["run_id"]
        dag_run = context["dag_run"]

        # --- Root cause robusto: leer TI desde DB ---
        session = Session()
        tis = (
            session.query(TaskInstance)
            .filter(TaskInstance.dag_id == dag_id, TaskInstance.run_id == run_id)
            .all()
        )
        session.close()

        exclude = {"log_run_start", "log_run_end"}
        tis = [ti for ti in tis if ti.task_id not in exclude]

        # Conteo por estado para diagnóstico
        state_counts = {}
        for ti in tis:
            state_counts[ti.state or "none"] = state_counts.get(ti.state or "none", 0) + 1

        # Prioridad de “culpable”
        priority = [
            State.FAILED,
            State.UPSTREAM_FAILED,
            "shutdown",          # algunos entornos lo registran como string
            "restarting",
            "removed",
            "none",              # tasks nunca corridas
        ]

        culprit = None
        for p in priority:
            for ti in tis:
                st = ti.state or "none"
                if st == p:
                    culprit = (st, ti.task_id)
                    break
            if culprit:
                break

        states = [ti.state for ti in tis]

        if any(s == State.FAILED for s in states):
            final_status = "FAILED"
            culprit = next(ti for ti in tis if ti.state == State.FAILED)
            error_message = f"FAILED task: {culprit.task_id}"

        elif any(s == State.UPSTREAM_FAILED for s in states):
            final_status = "FAILED"
            culprit = next(ti for ti in tis if ti.state == State.UPSTREAM_FAILED)
            error_message = f"UPSTREAM_FAILED task: {culprit.task_id}"

        elif all((s == State.SUCCESS) for s in states):
            final_status = "SUCCESS"
            error_message = None

        else:
            # estados mixtos: none/running/skipped, etc
            final_status = "FAILED"
            error_message = f"Incomplete run; states={state_counts}"

        # Métricas (pueden ser parciales)
        raw_repos_batches = hook.get_first("SELECT COUNT(*) FROM raw.github_repos;")[0]
        raw_commits_batches = hook.get_first("SELECT COUNT(*) FROM raw.github_commits;")[0]
        stg_repos_rows = hook.get_first("SELECT COUNT(*) FROM stg.repos;")[0]
        stg_commits_rows = hook.get_first("SELECT COUNT(*) FROM stg.commits;")[0]
        dim_repository_rows = hook.get_first("SELECT COUNT(*) FROM dw.dim_repository;")[0]
        fact_commits_rows = hook.get_first("SELECT COUNT(*) FROM dw.fact_commits;")[0]

        hook.run(
            """
            UPDATE dw.etl_run_log
            SET
            end_ts = NOW(),
            status = %s,
            error_message = %s,
            raw_repos_batches = %s,
            raw_commits_batches = %s,
            stg_repos_rows = %s,
            stg_commits_rows = %s,
            dim_repository_rows = %s,
            fact_commits_rows = %s
            WHERE pipeline_name = %s AND dag_id = %s AND run_id = %s;
            """,
            parameters=(
                final_status,
                error_message,
                raw_repos_batches,
                raw_commits_batches,
                stg_repos_rows,
                stg_commits_rows,
                dim_repository_rows,
                fact_commits_rows,
                PIPELINE_NAME,
                dag_id,
                run_id,
            ),
        )
    
    start_info = log_run_start()

    dq_fact_repo_id_not_null = SQLCheckOperator(
        task_id="dq_fact_repo_id_not_null",
        conn_id=POSTGRES_CONN_ID,
        sql="70_dq_fact_repo_id_not_null.sql",
    )

    dq_fact_repo_fk_dim = SQLCheckOperator(
        task_id="dq_fact_repo_fk_dim",
        conn_id=POSTGRES_CONN_ID,
        sql="71_dq_fact_repo_fk_dim.sql",
    )

    dq_watermark_not_ahead_of_fact = SQLCheckOperator(
        task_id="dq_watermark_not_ahead_of_fact",
        conn_id=POSTGRES_CONN_ID,
        sql="72_dq_watermark_not_ahead_of_fact.sql",
    )

    dq_last_raw_batches_have_effect = SQLCheckOperator(
        task_id="dq_last_raw_batches_have_effect",
        conn_id=POSTGRES_CONN_ID,
        sql="73_dq_last_raw_batches_have_effect.sql",
    )


    

    (
        init_schema 
        >> create_control_tables 
        >> create_run_log_table 
        >> extract_raw 
        >> transform_repos 
        >> load_dim_repository 
        >> extract_commits_to_raw() 
        >> transform_commits 
        >> load_fact_commits 
        >> update_watermark
        >> [
            dq_fact_repo_id_not_null,
            dq_fact_repo_fk_dim,
            dq_watermark_not_ahead_of_fact,
            dq_last_raw_batches_have_effect,
            ] 
        >> log_run_end(start_info)
    )
    
