with
    fonte_categorias as (
        select distinct
            cast(productcategoryid as integer) as categoria_id
            , cast(name as string) as categoria_nome
        from
            {{ source('adw', 'productcategory') }}
    )

    select
        *
    from
        fonte_categorias