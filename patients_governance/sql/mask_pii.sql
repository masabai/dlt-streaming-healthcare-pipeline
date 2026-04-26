-- mask name
CREATE OR REPLACE FUNCTION patient_data_governance.silver.mask_name(name STRING)
RETURN CASE
  WHEN is_account_group_member('admin') THEN name
  ELSE 'REDACTED'
END;

-- mask dob
CREATE OR REPLACE FUNCTION patients_governance.silver.mask_dob(dob DATE)
RETURN CASE
  WHEN is_account_group_member('admin') THEN dob
  ELSE CAST(NULL AS DATE)
END;

-- apply mask
ALTER TABLE patients_governance.silver.patients_silver
ALTER COLUMN first_name SET MASK patient_data_governance.silver.mask_name;

ALTER TABLE patients_governance.silver.patients_silver
ALTER COLUMN first_name SET MASK patient_data_governance.silver.mask_name;

ALTER TABLE patients_governance.silver.patients_silver
ALTER COLUMN birth_date SET MASK patients_governance.silver.mask_dob;