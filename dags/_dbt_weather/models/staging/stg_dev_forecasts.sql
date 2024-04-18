with stg_forecasts as (
select 
  -- id,
  DISTINCT valid_time as jour_heure_prevision,
  lat as latitude,
  long as longitude,
  REGEXP_REPLACE(name, ' \d+$', '') as commune,
  country as pays,
  temp_c as temperature,
  humidity as humidite,
  precip_mm as precipitation,
  pressure_mb as pression,
  wind_dir as direction_vent,
  wind_speed_kph as vitesse_vent,
  date::date,
  hour as heure,
  day as jour,
  month as mois
from {{ source('dev', 'forecasts') }}
)

select 
  *
from stg_forecasts