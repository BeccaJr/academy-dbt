select
    cast(min(cast(orderdate as timestamp)) as date) as data_min
    , cast(max(cast(orderdate as timestamp)) as date) as data_max
from
    {{ source('adw', 'salesorderheader') }}