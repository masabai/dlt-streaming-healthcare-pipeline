CREATE OR REPLACE VIEW gold.eda_top_conditions AS
SELECT
  description,
  COUNT(*) AS cnt
FROM patient_data_governance.silver.allergies
GROUP BY description
ORDER BY cnt DESC
LIMIT 20;