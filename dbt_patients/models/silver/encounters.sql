SELECT
    {{ snake_case_columns(source('dbt_patients', 'encounters')) }}
FROM {{ source('dbt_patients', 'encounters') }}