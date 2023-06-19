with
    fonte_motivo_vendas as (
        select distinct
            cast(salesreasonid as integer) as motivo_venda_id
            , name as motivo_venda_nome
            , reasontype as motivo_venda_tipo
        from
            {{ source('adw', 'salesreason') }}
    )

    select
        *
    from
        fonte_motivo_vendas