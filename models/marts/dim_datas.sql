with
    datas as (
        select
            *
        from
            {{ ref('stg_sap_adw__datas') }}
    ),

    transformacoes as (
        select
            data_id as data_sk
            , dia
            , mes
            , ano
            , dia_semana
            , final_de_semana
        from
            datas
    )

    select
        *
    from
        transformacoes