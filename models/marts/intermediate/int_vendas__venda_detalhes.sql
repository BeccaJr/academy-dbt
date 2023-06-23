with
    vendas as (
        select
            venda_id
            , cliente_id
            , endereco_id
            , cartao_id
            , venda_data
            , venda_status
        from
            {{ ref('stg_adw__vendas') }}
    ),

    venda_detalhes as (
        select
            venda_detalhe_id
            , venda_id
            , produto_id
            , venda_qtd
            , preco_unitario
        from
            {{ ref('stg_adw__venda_detalhes') }}
    ),

    join_tabelas as (
        select
            vd.venda_detalhe_id
            , vd.venda_id
            , vd.produto_id
            , v.cliente_id
            , v.endereco_id
            , v.cartao_id
            , vd.venda_qtd
            , vd.preco_unitario
            , v.venda_data
            , v.venda_status
        from
            venda_detalhes vd
        left join vendas v
            on vd.venda_id = v.venda_id
    )

    select
        *
    from
        join_tabelas