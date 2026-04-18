ALTER TABLE patient_data_governance.testing.claims
ALTER COLUMN Id SET TAGS ('classification' = 'identifier');

ALTER TABLE patient_data_governance.testing.claims
ALTER COLUMN PATIENTID SET TAGS ('classification' = 'PII_LINKED');

ALTER TABLE patient_data_governance.testing.claims
ALTER COLUMN PROVIDERID SET TAGS ('classification' = 'internal_reference');