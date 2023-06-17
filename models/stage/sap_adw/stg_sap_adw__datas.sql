with
    datas_min_max as (
        select 
            datas
        from (
            select
                cast(min(cast(orderdate as timestamp)) as date) as data_min,
                cast(max(cast(orderdate as timestamp)) as date) as data_max
            from
                {{ source('adw', 'salesorderheader') }}
            ) t
        join
            unnest(generate_date_array(t.data_min, t.data_max, interval 1 day)) datas
    ),
    datas_mes_ano as (
        select
            datas,
            extract(day from datas) as dia,
            extract(month from datas) as mes,
            extract(year from datas) as ano,
            extract(dayofweek from datas) as dia_semana,
            if(extract(dayofweek from datas) = 1 or extract(dayofweek from datas) = 7, true, false) as final_de_semana
        from
            datas_min_max
    )
select
    *
from
    datas_mes_ano