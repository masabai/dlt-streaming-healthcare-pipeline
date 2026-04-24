SELECT
  description,
  COUNT(*) AS total_prescriptions
FROM patients_governance.bronze.medications
GROUP BY description
ORDER BY total_prescriptions DESC
LIMIT 15