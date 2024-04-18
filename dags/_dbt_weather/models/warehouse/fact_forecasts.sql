with forecast as (
select
  sdf.temperature,
  sdf.humidite,
  sdf.precipitation,
  sdf.pression,
  sdf.direction_vent,
  sdf.vitesse_vent,
  dl.id,
  dt.jour_heure_prevision
from {{ ref('stg_dev_forecasts') }} as sdf
join {{ ref('dim_lieu') }} as dl on dl.nom_lieu = sdf.commune
join {{ ref('dim_temps') }} as dt on dt.jour_heure_prevision = sdf.jour_heure_prevision
)

select 
  temperature,
  humidite,
  precipitation,
  pression,
  direction_vent,
  vitesse_vent,
  id as lieu_id,
  jour_heure_prevision as temps_jour_heure
from
  forecast