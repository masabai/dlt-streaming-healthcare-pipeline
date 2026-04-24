%python
import great_expectations as gx
from great_expectations.expectations import *
import pandas as pd

# Initialize Context
context = gx.get_context()

# Setup Volume paths for the 3 tables
table_configs = {
    "patients": "/Volumes/patients_governance/bronze/raw_data/csv/patients.csv",
    "encounters": "/Volumes/patients_governance/bronze/raw_data/csv/encounters.csv",
    "claims": "/Volumes/patients_governance/bronze/raw_data/csv/claims.csv"
}

# Runs for all 3 tables
for table_name, path in table_configs.items():
    print(f"\n--- Validating Table: {table_name} ---")

    try:
        # Load CSV into Pandas ( to Bypasses Serverless Spark errors)
        df_pd = pd.read_csv(path)

        # Setup Data Source & Asset
        datasource = context.data_sources.add_or_update_pandas(name=f"ds_{table_name}")
        data_asset = datasource.add_dataframe_asset(name=f"asset_{table_name}")
        batch_def = data_asset.add_batch_definition_whole_dataframe(f"batch_{table_name}")

        # Define the Suite
        suite = context.suites.add_or_update(gx.ExpectationSuite(name=f"{table_name}_suite"))

        # Add Expectations (Dynamically match the file's own columns)
        actual_columns = list(df_pd.columns)
        suite.add_expectation(ExpectTableColumnsToMatchSet(column_set=actual_columns))

        # Add a custom rule for the ID column
        id_col = "Id" if "Id" in actual_columns else actual_columns[0]
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column=id_col))

        # Run Validation
        validation_def = gx.ValidationDefinition(
            name=f"val_{table_name}",
            data=batch_def,
            suite=suite
        )

        results = validation_def.run(batch_parameters={"dataframe": df_pd})

        # Print Results
        if results.success:
            print(f"{table_name.upper()} passed validation!")
        else:
            print(f"{table_name.upper()} FAILED. Summary of errors:")
            for val_res in results.results:
                if not val_res.success:
                    rule = getattr(val_res.expectation_config, "type", "Unknown")
                    col = val_res.expectation_config.kwargs.get("column", "Table")
                    print(f"   > Rule '{rule}' failed on {col}")

    except Exception as e:
        print(f"Error processing {table_name}: {str(e)}")
