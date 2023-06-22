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
        inner join vendas v
            on vd.venda_id = v.venda_id
    ),

    transformacoes as (
        select
            farm_fingerprint(cast(venda_detalhe_id as string)) as venda_detalhe_sk
            , farm_fingerprint(cast(venda_id as string)) as venda_sk
            , farm_fingerprint(cast(produto_id as string)) as produto_sk
            , farm_fingerprint(cast(cliente_id as string)) as cliente_sk
            , farm_fingerprint(cast(endereco_id as string)) as endereco_sk
            , farm_fingerprint(cast(cartao_id as string)) as cartao_sk
            , farm_fingerprint(cast(venda_data as string)) as data_sk
            , venda_qtd
            , preco_unitario
            , venda_status
        from
            join_tabelas
    )

    select
        *
    from
        transformacoes