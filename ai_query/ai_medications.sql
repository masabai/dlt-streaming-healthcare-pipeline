CREATE OR REPLACE VIEW patient_data_governance.gold.ai_enriched_medications AS
SELECT
  description,
  ai_query(
    'databricks-qwen3-next-80b-a3b-instruct',
    'Return exactly ONE short English phrase (max 12 words). No punctuation. No explanation. Define medication use: '
    || description
  ) AS medication_meaning
FROM patient_data_governance.silver.medications;