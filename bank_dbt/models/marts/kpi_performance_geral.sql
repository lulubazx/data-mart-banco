with investments as (
    select user_id, sum(valor_investido) as total_investido
    from {{ ref('stg_investments') }}
    group by user_id
),

loans as (
    select user_id, sum(valor_emprestimo) as total_divida
    from {{ ref('stg_loans') }}
    where status = 'active'
    group by user_id
),

cards as (
    select user_id, sum(valor_transacao) as gastos_cartao
    from {{ ref('stg_cards') }}
    group by user_id
)

select
    u.user_id,
    u.nome_completo,
    coalesce(i.total_investido, 0) as total_investido,
    coalesce(l.total_divida, 0) as total_divida_ativa,
    coalesce(c.gastos_cartao, 0) as gastos_totais_cartao
from {{ ref('stg_users') }} u
left join investments i on u.user_id = i.user_id
left join loans l on u.user_id = l.user_id
left join cards c on u.user_id = c.user_id