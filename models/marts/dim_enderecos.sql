with
    enderecos as (
        select
            cast(endereco_id as integer) as endereco_sk
            , cast(estado_id as integer) as estado_id
            , cidade_nome
        from
            {{ ref('stg_adw__enderecos') }}
    ),

    estados as (
        select
            estado_id
            , regiao_id
            , estado_nome
        from
            {{ ref('stg_adw__estados') }}
    ),

    regioes as (
        select
            regiao_id
            , regiao_nome
        from
            {{ ref('stg_adw__regioes') }}
    ),

    join_estado_regiao as (
        select
            *
        from
            estados e
        inner join regioes r
            on e.regiao_id = r.regiao_id
    ),

    transformacoes as (
        select
            farm_fingerprint(cast(e.endereco_sk as string)) as endereco_sk
            , e.endereco_sk as endereco_id
            , e.cidade_nome
            , er.estado_nome
            , er.regiao_nome
        from
            enderecos e
        left join join_estado_regiao er on
            e.estado_id = er.estado_id
    )

    select
        *
    from
        transformacoes