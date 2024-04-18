with lieu as (
select 
  lieu_id::int as id,
  nom_lieu,
  latitude::numeric,
  longitude::numeric,
  departement::text,
  region::text,
  code_postal::text
from {{ ref('stg_dev_geolocalisation') }}
)

select 
  *
FROM
  lieu