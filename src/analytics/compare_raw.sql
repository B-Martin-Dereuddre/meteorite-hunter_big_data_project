-- Independent implementation: read Bronze directly with DuckDB.
WITH fields AS (
    SELECT fall AS status,
           try_cast("mass (g)" AS DOUBLE) AS mass,
           try_cast(year AS DOUBLE) AS yr,
           try_cast(reclat AS DOUBLE) AS lat,
           try_cast(reclong AS DOUBLE) AS lon
    FROM read_csv(?, header = true, all_varchar = true)
), normalized AS (
    SELECT status,
           CASE WHEN isfinite(mass) AND mass > 0 THEN mass END AS mass_g,
           CASE WHEN yr = floor(yr) AND yr BETWEEN 1 AND 2013 THEN yr END AS event_year,
           CASE WHEN lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 360
                     AND (lat <> 0 OR lon <> 0) THEN lat END AS latitude
    FROM fields
)
SELECT status, count(*) AS records, count(mass_g) AS valid_mass,
       median(mass_g) AS median_mass_g, avg(mass_g) AS mean_mass_g,
       count(event_year) AS valid_year, count(latitude) AS valid_gps,
       count(*) FILTER (WHERE latitude <= -60) AS south_60
FROM normalized
GROUP BY status
ORDER BY status
