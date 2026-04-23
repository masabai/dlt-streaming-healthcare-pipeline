
CREATE OR REPLACE VIEW gold.eda_ai_summary_length AS
SELECT
  AVG(LENGTH(ai_summary)) AS avg_len,
  MIN(LENGTH(ai_summary)) AS min_len,
  MAX(LENGTH(ai_summary)) AS max_len
FROM patients_governance.gold.allergies;