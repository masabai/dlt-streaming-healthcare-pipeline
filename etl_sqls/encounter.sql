ALTER TABLE patient_data_governance.silver.encounters
ALTER COLUMN Id SET TAGS ('classification' = 'identifier');

ALTER TABLE patient_data_governance.silver.encounters
ALTER COLUMN START SET TAGS ('classification' = 'timestamp');

ALTER TABLE patient_data_governance.silver.encounters
ALTER COLUMN STOP SET TAGS ('classification' = 'timestamp');