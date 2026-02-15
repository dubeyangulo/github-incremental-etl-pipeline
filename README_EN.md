# 🚀 Incremental ETL Pipeline with Airflow -- Production-Style Data Architecture

## 📌 Project Overview

This project implements a **production-style incremental ETL pipeline**,
designed following modern data engineering best practices for enterprise
environments.

The pipeline extracts data from the GitHub REST API, transforms it, and
loads it into a dimensional Data Warehouse model, incorporating:

-   Incremental loading with per-entity watermark control
-   Idempotent data loads
-   Automatic deduplication
-   Built-in Data Quality checks
-   Run-level auditing and traceability
-   Automatic failure detection with root cause logging

The goal of this project is to simulate a real-world enterprise data
pipeline where reliability, observability, and data consistency are
critical.

------------------------------------------------------------------------

# 🏗 Architecture

## 🔄 General Flow

    GitHub API
         ↓
    RAW Layer (JSON storage)
         ↓
    STAGING Layer (Normalization + Deduplication)
         ↓
    DATA WAREHOUSE (Dimensional Model)
         ↓
    Data Quality & Run Audit

------------------------------------------------------------------------

## 🗂 Data Layers

### 1️⃣ RAW Layer

-   Full JSON payload persistence
-   Immutable ingestion
-   Enables reprocessing without additional API calls

Tables: - `raw.github_repos` - `raw.github_commits`

------------------------------------------------------------------------

### 2️⃣ STAGING Layer

-   Data normalization
-   Deduplication using window functions (`ROW_NUMBER()`)
-   Prevents PostgreSQL cardinality violations during UPSERT

Tables: - `stg.repos` - `stg.commits`

------------------------------------------------------------------------

### 3️⃣ Data Warehouse (Dimensional Model)

Optimized for analytics and reporting:

-   `dw.dim_repository`
-   `dw.fact_commits`

Relationship:

    fact_commits.repo_id → dim_repository.repo_id

This structure enables repository-level and time-based analysis of
commit activity.

------------------------------------------------------------------------

# 🔁 Incremental Strategy

The pipeline implements a robust incremental loading strategy based on:

-   Control table: `dw.etl_watermark`
-   Per-entity watermark tracking
-   Watermark updates only after successful FACT load
-   Protection against logical data loss

This approach prevents common production issues such as:

-   Skipping data due to intermediate failures
-   Inconsistent reprocessing
-   Premature state updates

------------------------------------------------------------------------

# 🧠 Key Engineering Decisions

### ✅ Idempotent Loads

The `stg.commits` table uses `ON CONFLICT` with natural key (`sha`),
ensuring:

-   Safe reprocessing
-   Consistency across repeated executions
-   Elimination of multi-batch duplicates

------------------------------------------------------------------------

### ✅ Window-Based Deduplication

Implemented using:

``` sql
ROW_NUMBER() OVER (PARTITION BY sha ORDER BY raw_ingested_at DESC)
```

This prevents errors such as:

    ON CONFLICT DO UPDATE command cannot affect row a second time

A common issue in real-world incremental pipelines.

------------------------------------------------------------------------

### ✅ Correct Watermark Handling

The watermark is updated only after:

    LOAD FACT → UPDATE WATERMARK

Never during extraction.

This protects against data loss when transformations or loads fail.

------------------------------------------------------------------------

# 📊 Integrated Data Quality Checks

The pipeline includes automated validation tasks that can stop execution
if inconsistencies are detected:

-   ✔ No NULL `repo_id` values in FACT
-   ✔ Referential integrity between FACT and DIM
-   ✔ Watermark consistency validation
-   ✔ RAW → STAGING propagation sanity check

If any validation fails:

-   The DAG is marked as FAILED
-   The root cause task is recorded
-   Full execution traceability is preserved

This aligns with modern Data Reliability Engineering practices.

------------------------------------------------------------------------

# 🧾 Run-Level Auditing & Observability

Table: `dw.etl_run_log`

Each execution records:

-   Run ID
-   Start and end timestamps
-   Status (RUNNING / SUCCESS / FAILED)
-   Root cause task (if failed)
-   Metrics per layer:
    -   RAW batches
    -   STAGING rows
    -   DIM rows
    -   FACT rows

This enables:

-   Historical traceability
-   Fast troubleshooting
-   Pipeline stability monitoring

------------------------------------------------------------------------

# 🐳 Local Execution

## Requirements

-   Docker
-   Docker Compose

## Start environment

``` bash
docker compose up -d
```

Airflow UI:

    http://localhost:8080

The DAG can be triggered manually or scheduled daily.

------------------------------------------------------------------------

# 🛠 Tech Stack

-   Python
-   Apache Airflow
-   PostgreSQL
-   Docker
-   GitHub REST API
-   Advanced SQL (CTEs, window functions, UPSERT)

------------------------------------------------------------------------

# 📈 Future Improvements

-   FACT table partitioning
-   Data freshness monitoring
-   Automated alerts (Slack/Email)
-   CI/CD integration
-   Incremental optimization by batch tracking
-   Monitoring dashboard (Power BI / Metabase)

------------------------------------------------------------------------

# 💼 Professional Objective

This project was developed to demonstrate:

-   Robust incremental pipeline design
-   Enterprise-grade data engineering practices
-   Data governance and reliability concepts
-   Observability in ETL workflows

It simulates a real-world enterprise data environment where consistency,
reliability, and traceability are priorities.

------------------------------------------------------------------------

# 👨‍💻 Author

Dubey Angulo\
Systems Engineer\
Colombia 🇨🇴


------------------------------------------------------------------------

# 🎯 Focus

This project demonstrates capabilities in:

-   Architectural thinking
-   Production-grade problem solving
-   Secure incremental data design
-   Failure management and auditing
-   Business-oriented data engineering
