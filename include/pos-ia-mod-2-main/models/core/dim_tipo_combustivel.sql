{{ config(schema='CORE_ONS', materialized='table') }}

with tipos_combustivel_base as (
  select nome_tipo_combustivel
  from {{ ref('stg_tipo_combustivel') }}
)

select
  md5(nome_tipo_combustivel) as sk_tipo_combustivel,
  nome_tipo_combustivel as nom_tipocombustivel,
  nome_tipo_combustivel as nome_tipo_combustivel_padronizado,
  case 
    when nome_tipo_combustivel like '%HIDRA%' or nome_tipo_combustivel like '%ÁGUA%' then 'Hidrelétrica'
    when nome_tipo_combustivel like '%EÓLIC%' or nome_tipo_combustivel like '%VENTO%' then 'Eólica'
    when nome_tipo_combustivel like '%SOLAR%' then 'Solar'
    when nome_tipo_combustivel like '%BIOMASS%' then 'Biomassa'
    when nome_tipo_combustivel like '%GÁS%' or nome_tipo_combustivel like '%GAS%' then 'Gás Natural'
    when nome_tipo_combustivel like '%CARVÃO%' or nome_tipo_combustivel like '%CARVAO%' then 'Carvão'
    when nome_tipo_combustivel like '%ÓLEO%' or nome_tipo_combustivel like '%OLEO%' or nome_tipo_combustivel like '%DIESEL%' then 'Derivados de Petróleo'
    when nome_tipo_combustivel like '%NUCLEAR%' then 'Nuclear'
    else 'Outros'
  end as fonte_energetica,
  case 
    when nome_tipo_combustivel like '%HIDRA%' or nome_tipo_combustivel like '%EÓLIC%' or nome_tipo_combustivel like '%SOLAR%' or nome_tipo_combustivel like '%BIOMASS%' then 'Renovável'
    else 'Não Renovável'
  end as classificacao_renovavel,
  current_timestamp() as data_criacao,
  current_timestamp() as data_atualizacao
from tipos_combustivel_base
