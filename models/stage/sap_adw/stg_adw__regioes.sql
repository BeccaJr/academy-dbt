with
    fonte_regioes as (
        select distinct
            cast(countryregioncode as string) as regiao_id
            , cast(name as string) as regiao_nome
        from
            {{ source('adw', 'countryregion') }}
    )

    select
        *
    from
        fonte_regioes