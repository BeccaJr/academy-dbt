with
    fonte_clientes as (
        select distinct
            cast(customerid as integer) as cliente_id
            , cast(personid as integer) as pessoa_id
            , cast(storeid as integer) as loja_id
        from
            {{ source('adw', 'customer') }}
    )

    select
        *
    from
        fonte_clientes