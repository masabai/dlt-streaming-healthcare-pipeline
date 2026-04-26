## Phase I: Healthcare Lakehouse with dbt-based ETL

Earlier version of this project used a dbt-based ETL pipeline on Databricks.

- Built Bronze → Silver → Gold layers using dbt models  
- Used `dbt run` for transformations across layers  
- Applied basic data tests (`not_null`, `unique`) on key tables like patients, encounters, and claims  
- Implemented column-level data masking for sensitive PHI (DOB) using Unity Catalog role-based access control
- Pipeline logic was spread across multiple dbt models
- Added the agent task to populate Unity Catalog column comments with AI-generated descriptions using `openai/gpt-oss-120b` via Groq

  
### Figure 1: ETL job run 
  - [![ETL Job run Screenshot](docs/etl_job_run.png)](docs/etl_job_run.png)

### Figure 2: Mask PII 
  - [![ETL Mask PII Screenshot](docs/mask_pii.png)](docs/mask_pii.png)


### Figure 3: Dashboard - Patient Condition Density vs Age  
  - [![ETL Scatter Plot Screenshot](docs/age_scatter.png)](docs/age_scatter.png)


### Figure 4: Dashboard - op Medications  
  - [![ETL Bar Graph Screenshot](docs/top_medications.png)](docs/top_medications.png)


## Phase II: Healthcare Lakehouse with Delta Live Tables (DLT)

**Built a declarative healthcare data pipeline using Delta Live Tables with integrated data quality and streaming ingestion.**

---

### Overview

This project implements a **Medallion Architecture (Bronze → Silver → Gold)** on Databricks using **Delta Live Tables (DLT)**
, a lakehouse pipeline with streaming ingestion, automated data quality enforcement, and AI-assisted metadata enrichment.
It replaces the earlier job-based ETL flow with a declarative pipeline where orchestration and data quality are handled within DLT.

It demonstrates how to build a **production-style data pipeline** with:
- Streaming ingestion (Auto Loader)
- Built-in data quality validation
- Declarative transformations
- Analytics-ready outputs

---

### Architecture


```mermaid
graph LR
    subgraph Raw
        A[Synthea CSVs]
    end

    subgraph Bronze
        B[Auto Loader]
    end

    subgraph Silver
        C{Data Quality}
        D[Standardized]
    end

    subgraph Gold
        E[Patient Summary]
        F[Financials]
        G[Top Trends]
    end

    A --> B
    B --> C
    C -->|Clean| D
    D --> E & F & G

    %% Styling for compact look
    style Bronze fill:#cd7f32,stroke:#333,color:#fff
    style Silver fill:#c0c0c0,stroke:#333
    style Gold fill:#ffd700,stroke:#333
```
#### Bronze — Raw Ingestion
- Ingested Synthea healthcare CSV data using Auto Loader (`cloudFiles`)
- Applied data quality checks at ingestion

---

#### Silver — Data Cleaning & Standardization
- Cleaned and standardized datasets
- Handled nulls and type casting

---

#### Gold — Analytics Layer

**Built business-ready and AI-enriched tables for analytics and exploration.**

- AI-generated semantic mapping of healthcare description fields into human-readable meanings using `ai_query`
- Built aggregated EDA tables such as `top_conditions`
- Created `ai_summary_stats` for structured insights from text-heavy fields

---

### DLT Features Used

- `@dlt.table` for declarative pipeline
- `@dlt.expect` for data quality checks
- Automatic lineage and dependency handling
- Built-in orchestration (no external scheduler)

---

### Design Decision

DLT made things easier because I didn’t have to manage job order or dependencies across multiple models. 
It just figures out the pipeline flow from the tables themselves.

### Figure 1: DLT Pipeline (Phase II)
- [![DLT Pipeline Screenshot](docs/delta_live_tables.png)](docs/delta_live_tables.png)