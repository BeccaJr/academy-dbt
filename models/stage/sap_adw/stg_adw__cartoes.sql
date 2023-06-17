with
    fonte_cartoes as (
        select distinct
            cast(creditcardid as integer) as cartao_id
            , cast(cardtype as string) as cartao_tipo
        from
            {{ source('adw', 'creditcard') }}
    )

    select
        *
    from
        fonte_cartoes