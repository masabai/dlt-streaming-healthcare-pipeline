SELECT
  p.id,
  ROUND(datediff(p.deathdate, p.birthdate)/365.25,0) AS age,
  COUNT(c.description) AS num_conditions
FROM patients_governance.bronze.patients p
JOIN patients_governance.bronze.conditions c
  ON p.id = c.patient
GROUP BY p.id, p.birthdate, p.deathdate