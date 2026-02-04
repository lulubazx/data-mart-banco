with source as (
    select * from {{ source('bank_raw', 'users') }}
),

renamed as (
    select
        id as user_id,
        name as nome_completo,
        email,
        created_at as data_cadastro
    from source
)

select * from renamed