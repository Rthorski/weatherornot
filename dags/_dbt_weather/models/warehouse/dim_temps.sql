with temps as (
select
  distinct (jour_heure_prevision),
  heure::INTEGER,
  jour::INTEGER,
  date::DATE,
  mois::INTEGER
FROM
  {{ ref('stg_dev_forecasts') }}
)

SELECT
  *
FROM
  temps
order BY
  jour_heure_prevision