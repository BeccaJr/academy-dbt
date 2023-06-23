with
    cartoes as (
        select
            *
        from
            {{ ref('dim_cartoes') }}
    ),

    clientes as (
        select
            *
        from
            {{ ref('dim_clientes') }}
    ),

    datas as (
        select
            *
        from
            {{ ref('dim_datas') }}
    ),

    enderecos as (
        select
            *
        from
            {{ ref('dim_enderecos') }}
    ),

    motivo_vendas as (
        select
            *
        from
            {{ ref('dim_motivo_vendas') }}
    ),

    produtos as (
        select
            *
        from
            {{ ref('dim_produtos') }}
    ),

    vendas_detalhes as (
        select
            *
        from
            {{ ref('int_vendas__venda_detalhes') }}
    ),

    join_tabelas as (
        select
            vd.venda_detalhe_id
            , vd.venda_id
            , mv.venda_sk as motivo_venda_sk
            , p.produto_sk
            , cl.cliente_sk
            , e.endereco_sk
            , c.cartao_sk
            , d.data_sk
            , p.produto_id
            , mv.motivo_venda_nome
            , d.data
            , vd.venda_qtd
            , vd.preco_unitario
            , vd.venda_status
        from 
            vendas_detalhes vd
        left join cartoes c
            on vd.cartao_id = c.cartao_id
        left join clientes cl
            on vd.cliente_id = cl.cliente_id
        left join datas d
            on vd.venda_data = d.data
        left join enderecos e
            on vd.endereco_id = e.endereco_id
        left join motivo_vendas mv
            on vd.venda_id = mv.venda_id
        left join produtos p
            on vd.produto_id = p.produto_id

    ),

    transformacoes as (
        select distinct
            farm_fingerprint(concat(cast(venda_detalhe_id as string), cast(produto_id as string))) as venda_detalhe_sk
            , farm_fingerprint(cast(venda_id as string)) as venda_sk
            , motivo_venda_sk
            , produto_sk
            , cliente_sk
            , endereco_sk
            , cartao_sk
            , data_sk
            , data
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