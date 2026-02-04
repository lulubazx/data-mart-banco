select *
from {{ ref('stg_loans') }}
where valor_emprestimo <= 0