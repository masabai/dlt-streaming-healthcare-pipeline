SELECT
  description,
  ai_query(
    'databricks-qwen3-next-80b-a3b-instruct',
    'Return ONE phrase in English (max 20 words): define this medical condition or injury: '
    || description
  ) AS condition_meaning
FROM patient_data_governance.silver.conditions
LIMIT 20;

CREATE OR REPLACE VIEW patient_data_governance.gold.ai_enriched_conditions AS
SELECT
  description,
  ai_query(
    'databricks-qwen3-next-80b-a3b-instruct',
    'Return exactly ONE short English phrase (max 12 words). No punctuation. No explanation. Define condition: '
    || description
  ) AS condition_meaning
FROM patient_data_governance.silver.conditions;