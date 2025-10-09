{{ config(schema='STAGING_ONS', materialized='view') }}

with raw as (
    select 
        ENA_DATA,
        ID_SUBSISTEMA,
        NOM_SUBSISTEMA,
        ENA_BRUTA_REGIAO_MWMED,
        ENA_ARMAZENAVEL_REGIAO_MWMED,
        ENA_BRUTA_REGIAO_PERCENTUALMLT,
        ENA_ARMAZENAVEL_REGIAO_PERCENTUALMLT,
        2023 as ano_referencia
    from {{ source('raw_ons', 'ENA_DIARIO_SUBSISTEMA_2023') }}
    
    union all
    
    select 
        ENA_DATA,
        ID_SUBSISTEMA,
        NOM_SUBSISTEMA,
        ENA_BRUTA_REGIAO_MWMED,
        ENA_ARMAZENAVEL_REGIAO_MWMED,
        ENA_BRUTA_REGIAO_PERCENTUALMLT,
        ENA_ARMAZENAVEL_REGIAO_PERCENTUALMLT,
        2024 as ano_referencia
    from {{ source('raw_ons', 'ENA_DIARIO_SUBSISTEMA_2024') }}
    
    union all
    
    select 
        ENA_DATA,
        ID_SUBSISTEMA,
        NOM_SUBSISTEMA,
        ENA_BRUTA_REGIAO_MWMED,
        ENA_ARMAZENAVEL_REGIAO_MWMED,
        ENA_BRUTA_REGIAO_PERCENTUALMLT,
        ENA_ARMAZENAVEL_REGIAO_PERCENTUALMLT,
        2025 as ano_referencia
    from {{ source('raw_ons', 'ENA_DIARIO_SUBSISTEMA_2025') }}
)

, tipado as (
    select
        ENA_DATA,
        ID_SUBSISTEMA,
        NOM_SUBSISTEMA,

        -- valores MWMED (mantidos)
        ENA_BRUTA_REGIAO_MWMED,
        ENA_ARMAZENAVEL_REGIAO_MWMED,

        -- percentuais originais (texto)
        ENA_BRUTA_REGIAO_PERCENTUALMLT,
        ENA_ARMAZENAVEL_REGIAO_PERCENTUALMLT,

        -- percentuais normalizados (número)
        try_to_decimal(replace(ENA_BRUTA_REGIAO_PERCENTUALMLT, ',', '.'), 10, 2)
            as ENA_BRUTA_REGIAO_PERCENTUALMLT_NUM_RAW,
        try_to_decimal(replace(ENA_ARMAZENAVEL_REGIAO_PERCENTUALMLT, ',', '.'), 10, 2)
            as ENA_ARMAZENAVEL_REGIAO_PERCENTUALMLT_NUM_RAW,

        ano_referencia
    from raw
)

select
    ENA_DATA,
    ID_SUBSISTEMA,
    NOM_SUBSISTEMA,

    ENA_BRUTA_REGIAO_MWMED,
    ENA_ARMAZENAVEL_REGIAO_MWMED,

    ENA_BRUTA_REGIAO_PERCENTUALMLT,
    ENA_ARMAZENAVEL_REGIAO_PERCENTUALMLT,

    -- percentuais numéricos com teto 100 e piso 0
    LEAST(GREATEST(ENA_BRUTA_REGIAO_PERCENTUALMLT_NUM_RAW, 0), 100)
        as ENA_BRUTA_REGIAO_PERCENTUALMLT_NUM,
    LEAST(GREATEST(ENA_ARMAZENAVEL_REGIAO_PERCENTUALMLT_NUM_RAW, 0), 100)
        as ENA_ARMAZENAVEL_REGIAO_PERCENTUALMLT_NUM,

    ano_referencia
from tipado
