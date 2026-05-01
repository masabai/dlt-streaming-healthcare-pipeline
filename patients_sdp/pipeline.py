from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from pyspark import pipelines as sdp

# ====================================================================================================
# Auto Loader - return live data frame
# ====================================================================================================
def build_bronze_stream(source_path: str, schema_location: str):
    """Create an Auto Loader streaming DataFrame from a CSV source."""
    return (
        spark.readStream.format("cloudFiles") #  Auto Loader is engine behind cloudFiles
        .option("cloudFiles.format", "csv") # file type
        .option("header", "true") # header as column names
        .option("cloudFiles.inferColumnTypes", "true") # infer schema not default to string type
        .option("cloudFiles.schemaLocation", schema_location) # location for schema inference-no need to manually create
        .option("cloudFiles.includeExistingFiles", "true")# process both new and existing files
        .option("rescuedDataColumn", "_rescued_data") # column for malformed records
        .load(source_path)# source path eg. s3/dfs/volume
    )

# ====================================================================================================
# BRONZE LAYER - Raw Ingestion
# ====================================================================================================
# allergies_bronze
@sdp.table(
    name="patient_delta.bronze.allergies_bronze",
    comment="Raw Synthea allergiess ingested via Auto Loader",
    table_properties={"quality": "bronze"}
)
# Verify data quality
@sdp.expect_or_fail("id_not_null", "CODE IS NOT NULL")
@sdp.expect("patient_not_null", "PATIENT IS NOT NULL")
@sdp.expect("start_not_null", "START IS NOT NULL")
def allergies_bronze():
    return build_bronze_stream(source_path="/Volumes/patient_delta/bronze/raw_data/csv/allergies/",
                               schema_location="/Volumes/patient_delta/bronze/raw_data/checkpoints/schema_allergies"
                               )

# claims_transactions_bronze
@sdp.table(
    name="patient_delta.bronze.claims_transactions_bronze",
    comment="Raw Synthea claims_transactions ingested via Auto Loader",
    table_properties={"quality": "bronze"}
)
# Verify data quality
@sdp.expect_or_fail("id_not_null", "Id IS NOT NULL")
@sdp.expect_or_fail("claim_id_not_null", "Claimid IS NOT NULL")
def claims_transactions_bronze():
    return build_bronze_stream(source_path="/Volumes/patient_delta/bronze/raw_data/csv/claims_transactions/",
                               schema_location="/Volumes/patient_delta/bronze/raw_data/checkpoints/schema_claims_transactions"
                               )

# claims_bronze
@sdp.table(
    name="patient_delta.bronze.claims_bronze",
    comment="Raw Synthea claims ingested via Auto Loader",
    table_properties={"quality": "bronze"}
)
# Verify data quality
@sdp.expect_or_fail("id_not_null", "Id IS NOT NULL")
@sdp.expect_or_fail("providerid_not_null", "providerId IS NOT NULL")
@sdp.expect("current_illness_date", "CURRENTILLNESSDATE <= current_date()")
@sdp.expect("patient_not_null", "PATIENTID IS NOT NULL")
@sdp.expect("provider_not_null", "PROVIDERID IS NOT NULL")
@sdp.expect("service_date_not_null", "SERVICEDATE IS NOT NULL")
def claims_bronze():
    return build_bronze_stream(source_path="/Volumes/patient_delta/bronze/raw_data/csv/claims/",
                               schema_location="/Volumes/patient_delta/bronze/raw_data/checkpoints/schema_claims"
                               )
# conditions_bronze
@sdp.table(
    name="patient_delta.bronze.conditions_bronze",
    comment="Raw Synthea conditions ingested via Auto Loader",
    table_properties={"quality": "bronze"}
)
# Verify data quality
@sdp.expect_or_fail("code_not_null", "CODE IS NOT NULL")
@sdp.expect("patient_not_null", "PATIENT IS NOT NULL")
@sdp.expect("start_not_null", "START IS NOT NULL")
def conditions_bronze():
    return build_bronze_stream(source_path="/Volumes/patient_delta/bronze/raw_data/csv/conditions/",
                               schema_location="/Volumes/patient_delta/bronze/raw_data/checkpoints/schema_conditions"
                               )

# encounters_bronze
@sdp.table(
    name="patient_delta.bronze.encounters_bronze",
    comment="Raw Synthea encounters ingested via Auto Loader",
    table_properties={"quality": "bronze"}
)
# Verify data quality
@sdp.expect_or_fail("id_not_null", "Id IS NOT NULL")
@sdp.expect("start_not_null", "START IS NOT NULL")
@sdp.expect("valid_start", "START <= current_timestamp()")
@sdp.expect("valid_stop", "STOP IS NULL OR STOP <= current_timestamp()")
@sdp.expect("start_before_stop", "STOP IS NULL OR START <= STOP")
@sdp.expect("non_negative_total_cost", "TOTAL_CLAIM_COST IS NULL OR TOTAL_CLAIM_COST >= 0")
@sdp.expect("non_negative_base_cost", "BASE_ENCOUNTER_COST IS NULL OR BASE_ENCOUNTER_COST >= 0")
@sdp.expect("non_negative_coverage", "PAYER_COVERAGE IS NULL OR PAYER_COVERAGE >= 0")
def encounters_bronze():
    return build_bronze_stream(source_path="/Volumes/patient_delta/bronze/raw_data/csv/encounters/",
                               schema_location="/Volumes/patient_delta/bronze/raw_data/checkpoints/schema_encounters"
                               )

# imaging_studies_bronze
@sdp.table(
    name="patient_delta.bronze.imaging_studies_bronze",
    comment="Raw Synthea conditions ingested via Auto Loader",
    table_properties={"quality": "bronze"}
)
# Verify data quality
@sdp.expect("sop_code_not_null", "SOP_CODE IS NOT NULL")
@sdp.expect("valid_patient", "patient IS NOT NULL")
def imaging_studies_bronze():
    return build_bronze_stream(source_path="/Volumes/patient_delta/bronze/raw_data/csv/imaging_studies/",
                               schema_location="/Volumes/patient_delta/bronze/raw_data/checkpoints/schema_imaging_studies"
                               )

# observations_bronze
@sdp.table(
    name="patient_delta.bronze.observations_bronze",
    comment="Raw Synthea observations ingested via Auto Loader",
    table_properties={"quality": "bronze"}
)
# Verify data quality
@sdp.expect_or_fail("code_not_null", "CODE IS NOT NULL")
@sdp.expect("patient_not_null", "PATIENT IS NOT NULL")
@sdp.expect("date_not_null", "DATE IS NOT NULL")
def observations_bronze():
    return build_bronze_stream(source_path="/Volumes/patient_delta/bronze/raw_data/csv/observations/",
                               schema_location="/Volumes/patient_delta/bronze/raw_data/checkpoints/schema_observations"
                               )

# patients_bronze
@sdp.table(
    name="patient_delta.bronze.patients_bronze",
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
    # call autoloader function
    return build_bronze_stream(source_path="/Volumes/patient_delta/bronze/raw_data/csv/patients/",
                               schema_location="/Volumes/patient_delta/bronze/raw_data/checkpoints/schema_patients"
                               )



# ====================================================================================================
# SILVER LAYER - DATA CLEANING
# ====================================================================================================
@sdp.table(
    name="patient_delta.silver.patients_silver",
    comment="Cleaned patients - Silver layer",
    table_properties={"quality": "silver"}
)
# patients_silver
def patients_silver():
    df = spark.read.table("patient_delta.bronze.patients_bronze")
    df = df.filter(col("Id").isNotNull())
    df = df.filter(col("GENDER").isin(['M', 'F', 'O', 'U'])) # spark is case-insensitve 'Id

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


# claims_silver
@sdp.table(
    name="patient_delta.silver.claims_silver",
    comment="Cleaned claims - Silver layer",
    table_properties={"quality": "silver"}
)
def claims_silver():
    df = spark.read.table("patient_delta.bronze.claims_bronze")
    df = df.filter(col("Id").isNotNull())
    df = df.filter(col("PATIENTID").isNotNull())
    df = df.filter(col("SERVICEDATE").isNotNull())

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
        col("lastbilleddate1").cast("timestamp").alias("last_billed_date_1"),
        col("lastbilleddate2").cast("timestamp").alias("last_billed_date_2"),
        col("lastbilleddatep").cast("timestamp").alias("last_billed_date_p"),

        # Status
        col("status1"),
        col("status2"),
        col("statusp"),

        # Financials
        col("outstanding1").cast("double"),
        col("outstanding2").cast("double"),
        col("outstandingp").cast("double"),

        # Claim type
        col("healthcareclaimtypeid1").cast("integer").alias("healthcare_claim_type_id_1"),
        col("healthcareclaimtypeid2").cast("integer").alias("healthcare_claim_type_id_2"),

        # Codes
        col("code").cast("integer"),

        # Metadata
        current_timestamp().alias("ingested_at")
    )


# encounters_silver
@sdp.table(
    name="patient_delta.silver.encounters_silver",
    comment="Cleaned encounters - Silver layer",
    table_properties={"quality": "silver"}
)
def encounters_silver():
    # Read Bronze
    df = spark.read.table("patient_delta.bronze.encounters_bronze")
    df = df.filter(col("Id").isNotNull())
    df = df.filter(col("PATIENT").isNotNull())
    df = df.filter(col("START").isNotNull())

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

# patient_summary
@sdp.table(
    name="patient_delta.gold.patient_summary",
    comment="Gold: unified patient health profile (clinical + risk context)"
)
def patient_summary():

    patients = spark.read.table("patient_delta.silver.patients_silver")

    patients.createOrReplaceTempView("patients")
    conditions = spark.read.table("patient_delta.bronze.conditions_bronze")
    conditions.createOrReplaceTempView("conditions")
    allergies = spark.read.table("patient_delta.bronze.allergies_bronze")
    allergies.createOrReplaceTempView("allergies")
    observations = spark.read.table("patient_delta.bronze.observations_bronze")
    observations.createOrReplaceTempView("observations")

    return spark.sql("""
        SELECT
            p.patient_id,
            p.first_name,
            p.last_name,
            p.gender,
            p.birth_date,
            p.race,
            p.ethnicity,

            COALESCE(c.condition_count, 0) AS condition_count,
            COALESCE(a.allergy_count, 0) AS allergy_count,
            COALESCE(o.observation_count, 0) AS observation_count

        FROM patients p

        LEFT JOIN (
            SELECT patient, COUNT(*) AS condition_count
            FROM conditions
            GROUP BY patient
        ) c
        ON p.patient_id = c.patient

        LEFT JOIN (
            SELECT patient, COUNT(*) AS allergy_count
            FROM allergies
            GROUP BY patient
        ) a
        ON p.patient_id = a.patient

        LEFT JOIN (
            SELECT patient, COUNT(*) AS observation_count
            FROM observations
            GROUP BY patient
        ) o
        ON p.patient_id = o.patient
    """)

# clinical_view
@sdp.table(
    name="patient_delta.gold.clinical_view",
    comment="Gold: clinical view with latest condition + imaging per patient"
)

def clinical_view():

    encounters = spark.read.table("patient_delta.silver.encounters_silver")

    # Latest condition per patient
    cond_latest = spark.sql("""
        SELECT
            PATIENT AS patient_id,
            DESCRIPTION AS latest_condition
        FROM (
            SELECT
                PATIENT,
                DESCRIPTION,
                ROW_NUMBER() OVER (
                    PARTITION BY PATIENT
                    ORDER BY START DESC NULLS LAST
                ) AS rn
            FROM patient_delta.bronze.conditions_bronze
        )
        WHERE rn = 1
    """)

    # Latest imaging per patient
    img_latest = spark.sql("""
        SELECT
            PATIENT AS patient_id,
            SOP_DESCRIPTION AS latest_imaging
        FROM (
            SELECT
                PATIENT,
                SOP_DESCRIPTION,
                ROW_NUMBER() OVER (
                    PARTITION BY PATIENT
                    ORDER BY DATE DESC NULLS LAST
                ) AS rn
            FROM patient_delta.bronze.imaging_studies_bronze
        )
        WHERE rn = 1
    """)

    # Final join
    return (
        encounters
        .join(cond_latest, "patient_id", "left")
        .join(img_latest, "patient_id", "left")
    )

# financial_view
@sdp.table(
    name="patient_delta.gold.financial_view",
    comment="Gold: claims + cost + utilization analysis (optimized)"
)

def financial_view():

    # BASE CLAIMS
    claims = spark.read.table("patient_delta.silver.claims_silver")

    # ENCOUNTERS (1 row per patient)
    enc = spark.read.table("patient_delta.silver.encounters_silver")

    w1 = Window.partitionBy("patient_id").orderBy(col("start_time").desc())

    enc_latest = (
        enc.withColumn("rn", row_number().over(w1))
        .filter(col("rn") == 1)
        .select(
            col("patient_id"),
            col("encounter_class")
        )
    )

    # TRANSACTIONS (1 row per claim)
    try:
        tx = spark.read.table("patient_delta.bronze.claims_transactions_bronze")

        w2 = Window.partitionBy("CLAIMID").orderBy(col("FROMDATE").desc())

        tx_latest = (
            tx.withColumn("rn", row_number().over(w2))
            .filter(col("rn") == 1)
            .select(
                col("CLAIMID").alias("claim_id"),
                col("AMOUNT").alias("latest_transaction_amount"),
                col("FROMDATE").alias("transaction_date"),
                col("OUTSTANDING").alias("transaction_outstanding"),
                col("PAYMENTS").alias("transaction_payments"),
                col("ADJUSTMENTS").alias("transaction_adjustments")
            )
        )

    except Exception:
        tx_latest = None

    base = claims.join(enc_latest, "patient_id", "left")

    if tx_latest is not None:
        base = base.join(tx_latest, "claim_id", "left")

    # FINAL OUTPUT
    return base.select(
        col("claim_id"),
        col("patient_id"),
        col("provider_id"),

        col("transaction_date"),
        col("latest_transaction_amount"),
        col("transaction_outstanding"),
        col("transaction_payments"),
        col("transaction_adjustments"),

        col("encounter_class")
 )

 # kpi_summary
@sdp.table(
    name="patient_delta.gold.kpi_summary",
    comment="Gold: high-level healthcare KPIs"
)
def kpi_summary():

    encounters = spark.read.table("patient_delta.silver.encounters_silver")

    return (
        encounters.groupBy("patient_id")
        .agg(
            count("*").alias("encounter_count"),
            avg("total_claim_cost").alias("avg_cost"),
            sum("total_claim_cost").alias("total_cost")
        )
    )