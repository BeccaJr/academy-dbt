with
    fonte_venda_motivo_vendas as (
        select distinct
            cast(venda_id as integer) as venda_sk
            , cast(motivo_venda_id as integer) as motivo_venda_id
        from
            {{ ref('stg_adw__vendas_motivo_vendas') }} 
            
            /*
            tem mais de um id por pedido -
             granularidade order item - entender como arrumar isso - 
             verificar order id = 51214
             
            select
            *
            from
            `forward-bee-344114.sap_adw.salesorderdetail` a
            inner join
            `forward-bee-344114.sap_adw.salesorderheadersalesreason` b
            on
            a.salesorderid = b.salesorderid
            where 
            a.salesorderid = 51214
            */
    ),

    fonte_motivo_vendas as (
        select
            cast(motivo_venda_id as integer) as motivo_venda_id
            , motivo_venda_nome
            , motivo_venda_tipo
        from
            {{ ref('stg_adw__motivo_vendas') }}
    ),

    transformacoes as (
        select distinct
            vmv.venda_sk
            , mv.motivo_venda_nome
            , mv.motivo_venda_tipo
        from
            fonte_venda_motivo_vendas vmv
        left join fonte_motivo_vendas mv on
            vmv.motivo_venda_id = mv.motivo_venda_id
    )

    select
        *
    from
        transformacoes