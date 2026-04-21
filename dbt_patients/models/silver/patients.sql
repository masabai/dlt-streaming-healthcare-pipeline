SELECT
    {{ snake_case_columns(source('dbt_patients', 'patients')) }}
from {{ source('dbt_patients', 'patients') }} -- name: from source.yml