with
    clientes as (
        select
            cliente_id
            , pessoa_id
            , loja_id
        from
            {{ ref('stg_adw__clientes') }}
    ),

    pessoas as (
        select
            pessoa_id
            , concat(pessoa_primeiro_nome, " ", pessoa_ultimo_nome) as pessoa_nome
        from
            {{ ref('stg_adw__pessoas') }}
    ),

    lojas as (
        select
            loja_id
            , loja_nome
        from
            {{ ref('stg_adw__lojas') }}
    ),

    join_tabelas as (
        select
            farm_fingerprint(cast(c.cliente_id as string)) as cliente_sk
            , cast(c.cliente_id as integer) as cliente_id
            , case
                when c.loja_id is not null then l.loja_nome
                else p.pessoa_nome
            end as cliente_nome
            , case
                when c.loja_id is not null then "Pessoa Jurídica"
                else "Pessoa Física"
            end as cliente_tipo
        from
            clientes c
        left join pessoas p
            on c.pessoa_id = p.pessoa_id
        left join lojas l
            on c.loja_id = l.loja_id
    )

    select
        *
    from
        join_tabelas