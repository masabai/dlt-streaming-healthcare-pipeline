
CREATE OR REPLACE VIEW patient_data_governance.gold.ai_enriched_allergies AS
SELECT
  description,
  ai_query(
    'databricks-qwen3-next-80b-a3b-instruct',
    'Write a short data catalog definition in 21 words max. No medical explanation, no paragraphs: '
    || description
  ) AS ai_summary
FROM patient_data_governance.silver.allergies;