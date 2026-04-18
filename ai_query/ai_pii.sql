
CREATE OR REPLACE VIEW patient_data_governance.gold.pii_audit_report AS
SELECT
  patient,
  description,
  ai_query(
    'databricks-qwen3-next-80b-a3b-instruct',
    'Act as a Data Auditor. Scan this text for PII (Names, SSN, or Phone).
     Return "SAFE" or "SENSITIVE". No other text: ' || description
  ) AS pii_flag
FROM patient_data_governance.silver.observations