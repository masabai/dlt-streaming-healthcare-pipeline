-- 1. Create the schemas (folders)
--USE CATALOG patient_data_governance;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS testing;

-- 2. Create the Volume (The storage bucket)
CREATE VOLUME IF NOT EXISTS bronze.raw_files;

-- Drop and recreate
DROP SCHEMA patient_data_governance.bronze CASCADE;
CREATE SCHEMA patient_data_governance.bronze;