import time
from openai import OpenAI

GROQ_API_KEY = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

# Setup Connection
client = OpenAI(
    api_key=GROQ_API_KEY,
    base_url="https://api.groq.com/openai/v1"
)

def get_ai_description(column_name, table_name):
    try:
        prompt = f"""
        Return a JSON object where keys are column names and values are 1-sentence descriptions.
        Table: {table_name}
        Columns: {column_name}
        """
        prompt = f"Write a 1-sentence professional description for the healthcare database column '{column_name}' in the table '{table_name}'. Focus on clinical data quality."

        response = client.chat.completions.create(
            model="openai/gpt-oss-120b",
            messages=[{"role": "user", "content": prompt}],
            timeout=10 # Don't let it hang forever
        )
        return response.choices[0].message.content.replace("'", "''")

    except Exception as e:
        return f"Healthcare attribute {column_name} (Auto-fallback due to: {str(e)[:50]})"

# Target the Silver Layer
testing_schema = "patient_data_governance.testing"
tables = spark.sql(f"SHOW TABLES IN {testing_schema}").collect()

for table in tables:
    t_name = table.tableName
    columns = spark.sql(f"DESCRIBE {testing_schema}.`{t_name}`").collect()

    print(f"--- AI Documenting Table: {t_name} ---")

    for col in columns:
        col_name = col.col_name
        if col_name.startswith('_') or col_name == "": continue

        # 3. Call the REAL Groq AI
        description = get_ai_description(col_name, t_name)

        # 4. Push to Unity Catalog
        try:
            spark.sql(f"ALTER TABLE {testing_schema}.`{t_name}` ALTER COLUMN `{col_name}` COMMENT '{description}'")
            print(f"  [AI-OK] {col_name}")

            # 5. The "Breath": Avoid Rate Limits (Important for Groq)
            time.sleep(0.5)

        except Exception as e:
            print(f"  [UC-ERROR] {col_name}: {e}")

print("\nPhase 2 Complete: LLM Metadata is live in Unity Catalog.")
