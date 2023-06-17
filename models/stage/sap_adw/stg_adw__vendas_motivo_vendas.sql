with
    fonte_venda_motivo_vendas as (
        select distinct
            cast(salesorderid as integer) as venda_id
            , cast(salesreasonid as integer) as motivo_venda_id
        from
            {{ source('adw', 'salesorderheadersalesreason') }}
    )

    select
        *
    from
        fonte_venda_motivo_vendas