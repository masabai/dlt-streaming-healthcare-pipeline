-- Create a mask: Only 'admins' see the real data, everyone else sees stars
CREATE OR REPLACE FUNCTION patient_data_governance.silver.mask_ssn(ssn STRING)
RETURN CASE
  WHEN is_account_group_member('admin') THEN ssn
  ELSE 'XXX-XX-XXXX'
END;

-- Apply it to Silver tables
ALTER TABLE patient_data_governance.silver.patients
ALTER COLUMN ssn SET MASK patient_data_governance.silver.mask_ssn;


--- Remove mask
ALTER TABLE patient_data_governance.silver.patients
ALTER COLUMN ssn DROP MASK;

-- Check mask removed
SHOW TBLPROPERTIES patient_data_governance.silver.patients;
OR
DESCRIBE TABLE EXTENDED patient_data_governance.silver.patients;