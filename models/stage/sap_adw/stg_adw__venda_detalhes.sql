select
    cast(salesorderdetailid as integer) as venda_detalhe_id
    , cast(salesorderid as integer) as venda_id
    , cast(productid as integer) as produto_id
    , cast(orderqty as integer) as venda_qtd
    , cast(unitprice as numeric) as preco_unitario
from
    {{ source('adw', 'salesorderdetail') }}