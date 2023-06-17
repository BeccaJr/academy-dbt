with
    fonte_produtos as (
        select distinct
            cast(productid as integer) as produto_id
            , cast(productsubcategoryid as integer) as produto_subcategoria_id
            , cast(name as string) as produto_nome
        from
            {{ source('adw', 'product') }}
    )

    select
        *
    from
        fonte_produtos