SELECT
    {{ snake_case_columns(source('dbt_patients', 'claims')) }}
from {{ source('dbt_patients', 'claims') }} -- name: from source.yml