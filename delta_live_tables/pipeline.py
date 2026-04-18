from pyspark.sql.functions import *
from pyspark import pipelines as sdp
import re


# ====================================================================================================
# Helper Functions
# ====================================================================================================
def build_bronze_stream(source_path: str, schema_location: str):
    """Create an Auto Loader streaming DataFrame from a CSV source."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.includeExistingFiles", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .load(source_path)
    )


def to_snake_case(df):
    """
    Converts DataFrame column names to snake_case safely.
    Example: FIRST_NAME -> first_name, HEALTHCAREEXPENSES -> healthcare_expenses
    """

    def convert(name: str) -> str:
        s1 = re.sub(r"[\s\-]+", "_", name)  # Replace non-alphanumeric with underscore
        s2 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", s1)  # Handle camelCase
        s3 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s2)  # Handle remaining lowercase

        return s3.lower()

    return df.toDF(*[convert(c) for c in df.columns])


# ====================================================================================================
# BRONZE LAYER - Raw Ingestion
# ====================================================================================================

@sdp.table(
    name="patient_data_governance.bronze.patients_bronze",
    comment="Raw Synthea patients ingested via Auto Loader",
    table_properties={"quality": "bronze"}
)
# Verify data quality
@sdp.expect_or_fail("id_not_null", "Id IS NOT NULL")
@sdp.expect("birth_date_not_null", "BIRTHDATE IS NOT NULL")
@sdp.expect("valid_birth_date", "BIRTHDATE <= current_date()")
@sdp.expect("non_negative_age", "DATEDIFF(current_date(), BIRTHDATE)/365 >= 0")
@sdp.expect("valid_gender", "GENDER IN ('M', 'F', 'O', 'U')")
@sdp.expect("no_future_death", "DEATHDATE IS NULL OR DEATHDATE <= current_date()")
def patients_bronze():
    return build_bronze_stream(source_path="/Volumes/patient_data_governance/bronze/raw_data/csv/patients/",
                               schema_location="/Volumes/patient_data_governance/bronze/raw_data/checkpoints/schema_patients"
                               )


@sdp.table(
    name="patient_data_governance.bronze.encounters_bronze",
    comment="Raw Synthea encounters ingested via Auto Loader",
    table_properties={"quality": "bronze"}
)
@sdp.expect_or_fail("id_not_null", "Id IS NOT NULL")
@sdp.expect("start_not_null", "START IS NOT NULL")
@sdp.expect("valid_start", "START <= current_timestamp()")
@sdp.expect("valid_stop", "STOP IS NULL OR STOP <= current_timestamp()")
@sdp.expect("start_before_stop", "STOP IS NULL OR START <= STOP")
@sdp.expect("non_negative_total_cost", "TOTAL_CLAIM_COST IS NULL OR TOTAL_CLAIM_COST >= 0")
@sdp.expect("non_negative_base_cost", "BASE_ENCOUNTER_COST IS NULL OR BASE_ENCOUNTER_COST >= 0")
@sdp.expect("non_negative_coverage", "PAYER_COVERAGE IS NULL OR PAYER_COVERAGE >= 0")
def encounters_bronze():
    return build_bronze_stream(source_path="/Volumes/patient_data_governance/bronze/raw_data/csv/encounters/",
                               schema_location="/Volumes/patient_data_governance/bronze/raw_data/checkpoints/schema_encounters"
                               )


@sdp.table(
    name="patient_data_governance.bronze.claims_bronze",
    comment="Raw Synthea claims ingested via Auto Loader",
    table_properties={"quality": "bronze"}
)
@sdp.expect_or_fail("id_not_null", "Id IS NOT NULL")
@sdp.expect("patient_not_null", "PATIENTID IS NOT NULL")
@sdp.expect("provider_not_null", "PROVIDERID IS NOT NULL")
@sdp.expect("service_date_not_null", "SERVICEDATE IS NOT NULL")
def claims_bronze():
    return build_bronze_stream(source_path="/Volumes/patient_data_governance/bronze/raw_data/csv/claims/",
                               schema_location="/Volumes/patient_data_governance/bronze/raw_data/checkpoints/schema_claims"
                               )


@sdp.table(
    name="patient_data_governance.bronze.allergies_bronze",
    comment="Raw Synthea allergiess ingested via Auto Loader",
    table_properties={"quality": "bronze"}
)
@sdp.expect_or_fail("id_not_null", "CODE IS NOT NULL")  #
@sdp.expect("patient_not_null", "PATIENT IS NOT NULL")
@sdp.expect("start_not_null", "START IS NOT NULL")
def allergies_bronze():
    return build_bronze_stream(source_path="/Volumes/patient_data_governance/bronze/raw_data/csv/allergies/",
                               schema_location="/Volumes/patient_data_governance/bronze/raw_data/checkpoints/schema_allergies"
                               )


@sdp.table(
    name="patient_data_governance.bronze.conditions_bronze",
    comment="Raw Synthea conditions ingested via Auto Loader",
    table_properties={"quality": "bronze"}
)
@sdp.expect_or_fail("code_not_null", "CODE IS NOT NULL")
@sdp.expect("patient_not_null", "PATIENT IS NOT NULL")
@sdp.expect("start_not_null", "START IS NOT NULL")
def conditions_bronze():
    return build_bronze_stream(source_path="/Volumes/patient_data_governance/bronze/raw_data/csv/conditions/",
                               schema_location="/Volumes/patient_data_governance/bronze/raw_data/checkpoints/schema_conditions"
                               )


@sdp.table(
    name="patient_data_governance.bronze.medications_bronze",
    comment="Raw Synthea medications ingested via Auto Loader",
    table_properties={"quality": "bronze"}
)
@sdp.expect_or_fail("code_not_null", "CODE IS NOT NULL")
@sdp.expect("patient_not_null", "PATIENT IS NOT NULL")
@sdp.expect("start_not_null", "START IS NOT NULL")
def medications_bronze():
    return build_bronze_stream(source_path="/Volumes/patient_data_governance/bronze/raw_data/csv/medications/",
                               schema_location="/Volumes/patient_data_governance/bronze/raw_data/checkpoints/schema_medications"
                               )


@sdp.table(
    name="patient_data_governance.bronze.observations_bronze",
    comment="Raw Synthea observations ingested via Auto Loader",
    table_properties={"quality": "bronze"}
)
@sdp.expect_or_fail("code_not_null", "CODE IS NOT NULL")
@sdp.expect("patient_not_null", "PATIENT IS NOT NULL")
@sdp.expect("date_not_null", "DATE IS NOT NULL")
def observations_bronze():
    return build_bronze_stream(source_path="/Volumes/patient_data_governance/bronze/raw_data/csv/observations/",
                               schema_location="/Volumes/patient_data_governance/bronze/raw_data/checkpoints/schema_observations"
                               )


# ====================================================================================================
# SILVER LAYER - DATA CLEANING
# ====================================================================================================
@sdp.table(
    name="patient_data_governance.testing.patients_silver",
    comment="Cleaned patients - Silver layer",
    table_properties={"quality": "testing"}
)
# TABLE : PATIENTS
def patients_silver():
    df = spark.read.table("patient_data_governance.bronze.patients_bronze")
    df = df.filter(col("Id").isNotNull())
    df = df.filter(col("GENDER").isin(['M', 'F', 'O', 'U']))
    df = to_snake_case(df)

    return df.select(
        col("id").cast("string").alias("patient_id"),
        col("first").alias("first_name"),
        col("last").alias("last_name"),
        col("gender").alias("gender"),
        col("birthdate").cast("date").alias("birth_date"),
        col("race").alias("race"),
        col("ethnicity").alias("ethnicity"),
        current_timestamp().alias("ingested_at")
    )


# TABLE : CLAIMS
@sdp.table(
    name="patient_data_governance.testing.claims_silver",
    comment="Cleaned claims - Silver layer",
    table_properties={"quality": "testing"}
)
def claims_silver():
    df = spark.read.table("patient_data_governance.bronze.claims_bronze")
    df = df.filter(col("Id").isNotNull())
    df = df.filter(col("PATIENTID").isNotNull())
    df = df.filter(col("SERVICEDATE").isNotNull())
    df = to_snake_case(df)

    return df.select(

        # Identifiers
        col("id").cast("string").alias("claim_id"),
        col("patientid").alias("patient_id"),
        col("providerid").alias("provider_id"),
        col("appointmentid").alias("appointment_id"),

        # Insurance
        col("primarypatientinsuranceid").alias("primary_insurance_id"),
        col("secondarypatientinsuranceid").alias("secondary_insurance_id"),

        # Department info
        col("departmentid").cast("integer").alias("department_id"),
        col("patientdepartmentid").cast("integer").alias("patient_department_id"),

        # Diagnoses
        col("diagnosis1").cast("long"),
        col("diagnosis2").cast("long"),
        col("diagnosis3").cast("long"),
        col("diagnosis4").cast("long"),
        col("diagnosis5").cast("long"),
        col("diagnosis6").cast("long"),
        col("diagnosis7").cast("long"),
        col("diagnosis8").cast("integer"),

        # Providers
        col("referringproviderid").alias("referring_provider_id"),
        col("supervisingproviderid").alias("supervising_provider_id"),

        # Dates
        col("currentillnessdate").cast("timestamp").alias("current_illness_date"),
        col("servicedate").cast("timestamp").alias("service_date"),
        col("lastbilleddate1").cast("timestamp"),
        col("lastbilleddate2").cast("timestamp"),
        col("lastbilleddatep").cast("timestamp"),

        # Status
        col("status1"),
        col("status2"),
        col("statusp"),

        # Financials
        col("outstanding1").cast("double"),
        col("outstanding2").cast("double"),
        col("outstandingp").cast("double"),

        # Claim type
        col("healthcareclaimtypeid1").cast("integer"),
        col("healthcareclaimtypeid2").cast("integer"),

        # Metadata
        current_timestamp().alias("ingested_at")
    )


# TABLE: ENCOUNTERS
@sdp.table(
    name="patient_data_governance.testing.encounters_silver",
    comment="Cleaned encounters - Silver layer",
    table_properties={"quality": "testing"}
)
def encounters_silver():
    # Read Bronze
    df = spark.read.table("patient_data_governance.bronze.encounters_bronze")
    df = df.filter(col("Id").isNotNull())
    df = df.filter(col("PATIENT").isNotNull())
    df = df.filter(col("START").isNotNull())
    df = to_snake_case(df)

    return df.select(

        # Identifiers
        col("id").cast("string").alias("encounter_id"),
        col("patient").alias("patient_id"),
        col("organization").alias("organization_id"),
        col("provider").alias("provider_id"),
        col("payer").alias("payer_id"),

        # Encounter details
        col("encounterclass").alias("encounter_class"),
        col("code").cast("integer").alias("encounter_code"),
        col("description").alias("description"),

        # Time fields
        col("start").cast("timestamp").alias("start_time"),
        col("stop").cast("timestamp").alias("stop_time"),

        # Costs (safe numeric casting)
        col("base_encounter_cost").cast("double").alias("base_encounter_cost"),
        col("total_claim_cost").cast("double").alias("total_claim_cost"),
        col("payer_coverage").cast("double").alias("payer_coverage"),

        # Clinical reason
        col("reasoncode").cast("long").alias("reason_code"),
        col("reasondescription").alias("reason_description"),

        # Metadata
        current_timestamp().alias("ingested_at")
    )


# ====================================================================================================
# GOLD LAYER
# ====================================================================================================

# Patients' summary
@sdp.table(
    name="patient_data_governance.gold.patient_summary",
    comment="Gold: patient-level health summary"
)
def patient_summary():
    patients = spark.read.table("patient_data_governance.testing.patients_silver")
    encounters = spark.read.table("patient_data_governance.testing.encounters_silver")

    enc_count = encounters.groupBy("patient_id").count()

    return patients.join(enc_count, "patient_id", "left").select(
        "patient_id",
        "first_name",
        "last_name",
        "gender",
        "birth_date",
        col("count").alias("encounter_count")
    )


# Encounters per patient
@sdp.table(
    name="patient_data_governance.gold.encounters_per_patient",
    comment="Gold: number of encounters per patient"
)
def encounters_per_patient():
    df = spark.read.table("patient_data_governance.testing.encounters_silver")

    return df.groupBy("patient_id").count().selectExpr(
        "patient_id",
        "count as encounter_count"
    )


# Claims cost per patient
@sdp.table(
    name="patient_data_governance.gold.claims_cost_per_patient",
    comment="Gold: total healthcare cost per patient"
)
def claims_cost_per_patient():
    df = spark.read.table("patient_data_governance.testing.claims_silver")

    return df.groupBy("patient_id").agg(
        {"outstanding1": "sum",
         "outstanding2": "sum",
         "outstandingp": "sum"}
    ).withColumnRenamed(
        "sum(outstanding1)", "total_outstanding_1"
    ).withColumnRenamed(
        "sum(outstanding2)", "total_outstanding_2"
    ).withColumnRenamed(
        "sum(outstandingp)", "total_outstanding_p"
    )


# Top Conditions : Most common dieases
@sdp.table(
    name="patient_data_governance.gold.top_conditions",
    comment="Gold: most frequent conditions"
)
def top_conditions():
    df = spark.read.table("patient_data_governance.bronze.conditions_bronze")

    return df.groupBy("description").count().orderBy("count", ascending=False)


# Top Medications
@sdp.table(
    name="patient_data_governance.gold.top_medications",
    comment="Gold: most prescribed medications"
)
def top_medications():
    df = spark.read.table("patient_data_governance.bronze.medications_bronze")

    return df.groupBy("description").count().orderBy("count", ascending=False)


# Allergy Frequency
@sdp.table(
    name="patient_data_governance.gold.top_allergies",
    comment="Gold: most common allergies"
)
def top_allergies():
    df = spark.read.table("patient_data_governance.bronze.allergies_bronze")

    return df.groupBy("description").count().orderBy("count", ascending=False)
