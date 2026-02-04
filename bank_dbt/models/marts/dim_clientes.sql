select
    user_id,
    nome_completo,
    email,
    data_cadastro
from {{ ref('stg_users') }}