with source as (
    select * from {{ source('bank_raw', 'loans') }}
),

renamed as (
    select
        id as loan_id,
        user_id,
        amount as valor_emprestimo,
        status,
        interest_rate as taxa_juros,
        created_at as data_solicitacao
    from source
)

select * from renamed