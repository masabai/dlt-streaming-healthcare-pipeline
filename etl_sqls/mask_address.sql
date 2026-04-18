CREATE OR REPLACE FUNCTION patient_data_governance.silver.mask_address(addr STRING)
RETURN CASE
  WHEN is_account_group_member('admin') THEN addr
  ELSE '***REDACTED***'
END;


ALTER TABLE patient_data_governance.silver.patients
ALTER COLUMN ADDRESS SET MASK patient_data_governance.silver.mask_address;