select * from {{ source('patients', 'allergies') }}
