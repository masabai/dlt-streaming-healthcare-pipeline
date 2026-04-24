SELECT
  description,
  COUNT(*) AS total_patients
FROM patients_governance.bronze.conditions
GROUP BY description
ORDER BY total_patients DESC
LIMIT 15