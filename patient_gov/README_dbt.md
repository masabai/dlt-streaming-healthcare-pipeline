# AI-Governed Healthcare Lakehouse (Synthea)

**“Migrated a manual CSV process to an automated, QA-validated, AI-assisted Lakehouse on Databricks.”**

---

## Phase 1: Data Infrastructure (Medallion Architecture with dbt)

Built a structured multi-layer data pipeline using dbt and Databricks.

### Medallion Architecture Design

Organized Synthea synthetic healthcare data into standard layers:

- **Bronze Layer:** Raw ingestion of Synthea CSV files with no transformations  
- **Silver Layer:** Cleaned and standardized datasets with basic validation  
- **Gold Layer:** Business-ready analytical models prepared for reporting and insights  

### dbt Integration

- Connected dbt to Databricks workspace  
- Used `dbt run` to execute transformations across layers  
- Implemented modular models for Bronze → Silver → Gold flow  

### Data Validation (QA Layer)

Implemented targeted data quality checks based on key healthcare entities.

- Applied dbt tests (`not_null`, `unique`) on selected core tables:
  - patients  
  - encounters  
  - claims  

- Focused validation on critical business entities rather than full schema coverage  
- Ensured basic data integrity for key analytical use cases  

---

## Phase 2: AI-Driven Metadata Governance (Unity Catalog)

Automated metadata enrichment for the Silver layer using LLM-generated descriptions.

### Automated Documentation

- Built Python-based notebook workflow in Databricks  
- Iterated through Silver-layer tables and columns  
- Used LLM API to generate consistent column-level descriptions to enforce domain-aware documentation
instead of platform defaults.
- Pushed AI-generated descriptions into Unity Catalog 
- Reduced manual effort for column-level data dictionary creation  

### Data Governance (Unity Catalog)

- Demonstrated data governance using Unity Catalog features such as role-based access and column-level controls to protect sensitive data
- Enhanced analytical value by using Databricks AI functions (e.g., `ai_query()`) within SQL views to enrich raw data for business insights
- Demonstrated how LLM-based functions can enrich raw healthcare data into analytical signals


### Priority Tiering (Metadata Classification)

- Applied governance-based tagging to classify tables by business importance  
- Defined priority tiers:
  - Tier 1: Core entities (patients, encounters, claims)  
  - Tier 2: Clinical supporting tables  
  - Tier 3: Reference / low-priority datasets  

- Implemented tagging using Unity Catalog metadata (table-level classification)
---

## Phase 3: Analytics & Dashboard Layer

Databricks SQL dashboard
Risk scoring / insights
business-facing outputs
### Outcome

- End-to-end Medallion architecture implemented  
- Automated metadata generation integrated into Unity Catalog  
- Targeted QA validation on key healthcare entities  
- Analytics-ready dataset with SQL-based reporting layer  

---

## Key Takeaway

> “Built a modern data engineering pipeline using dbt and Databricks that transforms raw synthetic 
> healthcare data into structured, validated, and analytics-ready datasets with AI-assisted metadata 
> generation.”