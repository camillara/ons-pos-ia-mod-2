{{ config(materialized='view', schema='STAGING_ONS') }}

with base as (
  select
    upper(trim(NOM_TIPOCOMBUSTIVEL)) as nome_tipo_combustivel
  from {{ source('raw_ons', 'DISPONIBILIDADE_USINA_GERAL') }}
  where NOM_TIPOCOMBUSTIVEL is not null and trim(NOM_TIPOCOMBUSTIVEL) <> ''
)
select distinct
  nome_tipo_combustivel
from base
