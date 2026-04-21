testing_schema = "patient_data_governance.silver"

tables = spark.sql(f"SHOW TABLES IN {testing_schema}").collect()


def get_priority(table_name):
    t = table_name.lower()

    # Tier 1: core clinical entities
    if "patients" in t or "encounters" in t or "claims" in t:
        return "tier_1"

    # Tier 2: supporting clinical data
    if "medications" in t or "conditions" in t or "allergies" in t:
        return "tier_2"

    # Tier 3: everything else
    return "tier_3"


for table in tables:
    t_name = table.tableName
    priority = get_priority(t_name)

    try:
        spark.sql(f"""
            ALTER TABLE {testing_schema}.`{t_name}`
            SET TAGS ('priority' = '{priority}')
        """)

        print(f"Tagged {t_name} -> {priority}")

    except Exception as e:
        print(f"ERROR tagging {t_name}: {e}")