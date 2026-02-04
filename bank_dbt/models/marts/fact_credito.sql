with loans as (
    select * from {{ ref('stg_loans') }}
),

clientes as (
    select * from {{ ref('stg_users') }}
)

select
    l.loan_id,
    l.user_id,
    c.nome_completo,
    l.valor_emprestimo,
    l.taxa_juros,
    l.status,
    -- Regra de Negócio: Flag de Risco
    case 
        when l.status = 'defaulted' then true 
        else false 
    end as is_inadimplente,
    l.data_solicitacao
from loans l
left join clientes c on l.user_id = c.user_id