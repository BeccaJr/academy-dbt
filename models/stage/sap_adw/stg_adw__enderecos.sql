with
    fonte_enderecos as (
        select distinct
            cast(addressid as integer) as endereco_id
            , cast(stateprovinceid as integer) as estado_id
            , cast(city as string) as cidade_nome
        from
            {{ source('adw', 'address') }}
    )

    select
        *
    from
        fonte_enderecos