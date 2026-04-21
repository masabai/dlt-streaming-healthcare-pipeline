%python
from pyspark.sql.functions import col, current_timestamp
import re


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


# TABLE : PATIENTS
def patients_silver():
    df = spark.read.table("patients_governance.bronze.patients")
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
def claims_silver():
    df = spark.read.table("patients_governance.bronze.claims")
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
def encounters_silver():
    # Read Bronze
    df = spark.read.table("patients_governance.bronze.encounters")
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


# Write cleaned tables in silver
df_patients = patients_silver()
df_patients.write.mode("overwrite").saveAsTable("patients_governance.silver.patients_silver")

df_claims = claims_silver()
df_claims.write.mode("overwrite").saveAsTable("patients_governance.silver.claims_silver")

df_encounters = encounters_silver()
df_encounters.write.mode("overwrite").saveAsTable("patients_governance.silver.encounters_silver")



