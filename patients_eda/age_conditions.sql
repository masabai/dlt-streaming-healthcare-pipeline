SELECT
  p.id,
  ROUND(datediff(p.deathdate, p.birthdate)/365.25,0) AS age,
  COUNT(c.description) AS num_conditions
FROM patients_governance.bronze.patients p
JOIN patients_governance.bronze.conditions c
  ON p.id = c.patient
GROUP BY p.id, p.birthdate, p.deathdate


/*
SELECT
  p.id,
  ROUND(datediff(p.deathdate, p.birthdate)/365.25,0) AS age,
  COUNT(c.description) AS num_conditions
FROM patient_delta.bronze.patients_bronze p
JOIN patient_delta.bronze.conditions_bronze c
  ON p.id = c.patient
GROUP BY p.id, p.birthdate, p.deathdate
HAVING ROUND(datediff(p.deathdate, p.birthdate)/365.25,0) > 18 --BETWEEN 50 AND 60
ORDER BY AGE
*/