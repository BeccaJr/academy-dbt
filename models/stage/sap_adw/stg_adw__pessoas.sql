with
    fonte_pessoas as (
        select distinct
            cast(businessentityid as integer) as pessoa_id
            , cast(firstname as string) as pessoa_primeiro_nome
            , cast(lastname as string) as pessoa_ultimo_nome
        from
            {{ source('adw', 'person') }}
    )

    select
        *
    from
        fonte_pessoas