select
    cast(salesorderid as integer) as venda_id
    , cast(customerid as integer) as cliente_id
    , cast(billtoaddressid as integer) as endereco_id
    , cast(creditcardid as integer) as cartao_id
    , cast(cast(orderdate as timestamp) as date) as venda_data
    , cast(status as integer) as venda_status
from
    {{ source('adw', 'salesorderheader') }}