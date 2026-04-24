%python
# Define volume paths
volume_path = "/Volumes/patients_governance/bronze/raw_data/csv/"
target_schema = "patients_governance.bronze"

# List all files in the volume folder
files = dbutils.fs.ls(volume_path)

for file in files:
    if file.name.endswith(".csv"):
        # Create a clean table name (e.g., 'patients' from 'patients.csv')
        table_name = file.name.replace(".csv", "").lower()

        print(f"Creating table: {target_schema}.{table_name}")

        # Read and save as a Delta Table
        (spark.read.format("csv")
         .option("header", "true")
         .option("inferSchema", "true")
         .load(file.path)
         .write.mode("overwrite")
         .saveAsTable(f"{target_schema}.{table_name}"))

print("--- PHASE 1 COMPLETE: 18 Tables in Bronze  ---")
