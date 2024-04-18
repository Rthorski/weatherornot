with stg_geolocalisation as (
select 
  lieu_id::int,
  dep_name as departement,
  reg_name as region,
  com_code as code_postal,
  com_name_lower as nom_lieu,
  latitude,
  longitude
from 
  {{ source('dev', 'geolocalisation') }}
)

select
  *
FROM stg_geolocalisation
