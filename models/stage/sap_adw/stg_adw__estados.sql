with
    fonte_estados as (
        select distinct
            cast(stateprovinceid as integer) as estado_id
            , cast(countryregioncode as string) as regiao_id
            , cast(name as string) as estado_nome
        from
            {{ source('adw', 'stateprovince') }}
    )

    select
        *
    from
        fonte_estados