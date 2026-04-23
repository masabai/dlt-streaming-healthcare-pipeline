{{ config(materialized='table') }}

SELECT
  description,
  ai_query(
    'databricks-qwen3-next-80b-a3b-instruct',
    'Return exactly ONE short English phrase (max 12 words). No punctuation. No explanation. Define condition: '
    || description
  ) AS condition_meaning
FROM {{ source('dbt_patients', 'allergies') }}