ALTER TABLE patient_data_governance.tesing.patients
ALTER COLUMN Id SET TAGS ('classification' = 'identifier');

ALTER TABLE patient_data_governance.testing.patients
ALTER COLUMN ssn SET TAGS ('classification' = 'PII');

ALTER TABLE patient_data_governance.testing.patients
ALTER COLUMN BIRTHDATE SET TAGS ('classification' = 'PII');

ALTER TABLE patient_data_governance.testing.patients
ALTER COLUMN DEATHDATE SET TAGS ('classification' = 'sensitive');