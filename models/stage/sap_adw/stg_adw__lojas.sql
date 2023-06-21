with
    fonte_lojas as (
        select distinct
            cast(businessentityid as integer) as loja_id
            , cast(name as string) as loja_nome
        from
            {{ source('adw', 'store') }}
    )

    select
        *
    from
        fonte_lojas