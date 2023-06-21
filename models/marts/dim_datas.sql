with
    datas_min_max as (
        select 
            datas
        from (
            select
                data_min
                , data_max
            from
                {{ ref('stg_sap_adw__datas') }}
            ) t
        join
            unnest(generate_date_array(t.data_min, t.data_max, interval 1 day)) datas
    ),

    datas_mes_ano as (
        select
            datas as data_id
            , extract(day from datas) as dia
            , extract(month from datas) as mes
            , extract(year from datas) as ano
            , extract(dayofweek from datas) as dia_semana
            , if(extract(dayofweek from datas) = 1 or extract(dayofweek from datas) = 7, true, false) as final_de_semana
        from
            datas_min_max
    ),

    transformacoes as (
        select
            farm_fingerprint(cast(data_id as string)) as data_sk
            , data_id as data
            , dia
            , mes
            , ano
            , dia_semana
            , final_de_semana
        from
            datas_mes_ano
    )

    select
        *
    from
        transformacoes