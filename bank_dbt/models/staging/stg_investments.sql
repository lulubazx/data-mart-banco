with source as (
    select * from {{ source('bank_raw', 'investments') }}
),

renamed as (
    select
        id as investment_id,
        user_id,
        type as tipo_investimento,
        amount as valor_investido,
        date_invested as data_investimento
    from source
)

select * from renamed