with source as (
    select * from {{ source('bank_raw', 'card_transactions') }}
),

renamed as (
    select
        id as transaction_id,
        user_id,
        amount as valor_transacao,
        category as categoria,
        transaction_date as data_transacao
    from source
)

select * from renamed