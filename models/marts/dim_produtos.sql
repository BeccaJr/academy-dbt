with
    produtos as (
        select
            cast(produto_id as integer) as produto_sk
            , cast(produto_subcategoria_id as integer) as produto_subcategoria_id
            , produto_nome
        from
            {{ ref('stg_sap_adw__produtos') }}
    ),

    categoria as (
        select
            categoria_id
            , categoria_nome
        from
            {{ ref('stg_adw__produtocategorias') }}
    ),

    subcategoria as (
        select
            subcategoria_id
            , categoria_id
            , subcategoria_nome
        from
            {{ ref('stg_adw__produtosubcategorias') }}
    ),

    join_categoria_subcategoria as (
        select
            *
        from
            subcategoria sc
        inner join categoria c
            on sc.categoria_id = c.categoria_id
    ),

    transformacoes as (
        select
            p.produto_sk
            , p.produto_nome
            , csc.subcategoria_nome
            , csc.categoria_nome
        from
            produtos p
        left join join_categoria_subcategoria csc on
            p.produto_subcategoria_id = csc.subcategoria_id
    )

    select
        *
    from
        transformacoes