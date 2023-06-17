with
    cartoes as (
        select
            *
        from
            {{ ref('stg_adw__cartoes') }}
    ),

    transformacoes as (
        select
            cast(cartao_id as integer) as cartao_sk
            , cast(cartao_tipo as string) as cartao_tipo
        from
            cartoes
    )

    select
        *
    from
        transformacoes