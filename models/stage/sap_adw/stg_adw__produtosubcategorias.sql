with
    fonte_subcategorias as (
        select distinct
            cast(productsubcategoryid as integer) as subcategoria_id
            , cast(productcategoryid as integer) as categoria_id
            , cast(name as string) as subcategoria_nome
        from
            {{ source('adw', 'productsubcategory') }}
    )

    select
        *
    from
        fonte_subcategorias