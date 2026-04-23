{{ config(materialized='table') }}

SELECT
  description,
  ai_query(
    'databricks-qwen3-next-80b-a3b-instruct',
    'Write a short data catalog definition in 21 words max. No medical explanation, no paragraphs: '
    || description
  ) AS ai_summary
FROM {{ source('dbt_patients', 'allergies') }}