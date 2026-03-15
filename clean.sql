START TRANSACTION;

-- ============================================================
-- clean.sql - Idempotent transform for Railway cron
-- This runs on every cron trigger. It rebuilds the
-- normalized tables from scratch using the staging data.
-- ============================================================

-- Step 1: Create tables if they do not exist yet (first run)
CREATE TABLE IF NOT EXISTS flight_snapshots (
    snapshot_id serial PRIMARY KEY,
    snapshot_time timestamptz NOT NULL,
    collected_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS flights (
    flight_id serial PRIMARY KEY,
    snapshot_id integer REFERENCES flight_snapshots (snapshot_id),
    icao24 varchar(10),
    callsign varchar(10),
    origin_country varchar(100),
    longitude numeric(9,4),
    latitude numeric(8,4),
    baro_altitude numeric(8,2),
    on_ground boolean,
    velocity numeric(7,2),
    true_track numeric(6,2),
    vertical_rate numeric(6,2),
    geo_altitude numeric(8,2),
    squawk varchar(10),
    spi boolean,
    position_source smallint
);

CREATE TABLE IF NOT EXISTS weather_observations (
    observation_id serial PRIMARY KEY,
    observation_time timestamptz NOT NULL,
    collected_at timestamptz NOT NULL,
    temperature_c numeric(5,2),
    wind_speed_kmh numeric(6,2),
    weather_code smallint,
    latitude numeric(8,4),
    longitude numeric(9,4),
    elevation numeric(6,1)
);

-- Step 2: Truncate in dependency order (children first)
-- RESTART IDENTITY resets the serial counters
-- CASCADE is not needed here because we truncate children first,
-- but it is a safety net
TRUNCATE flights RESTART IDENTITY;
TRUNCATE flight_snapshots RESTART IDENTITY CASCADE;
TRUNCATE weather_observations RESTART IDENTITY;

-- Step 3: Re-populate from staging data
-- (same INSERT INTO ... SELECT queries from the transform step)

-- Weather observations
INSERT INTO weather_observations (
    observation_time, collected_at, temperature_c,
    wind_speed_kmh, weather_code,
    latitude, longitude, elevation
)
SELECT DISTINCT
    (raw_json->'current'->>'time')::timestamptz,
    created_at,
    (raw_json->'current'->>'temperature_2m')::numeric,
    (raw_json->'current'->>'wind_speed_10m')::numeric,
    (raw_json->'current'->>'weathercode')::smallint,
    (raw_json->>'latitude')::numeric,
    (raw_json->>'longitude')::numeric,
    (raw_json->>'elevation')::numeric
FROM weather_json_data;

-- Flight snapshots (only rows where states is not null)
INSERT INTO flight_snapshots (snapshot_time, collected_at)
SELECT DISTINCT
    to_timestamp((raw_json->>'time')::bigint),
    created_at
FROM flight_json_data
WHERE raw_json->'states' IS NOT NULL
  AND raw_json->>'states' != 'null';

-- Flights (unnest the states array)
INSERT INTO flights (
    snapshot_id, icao24, callsign, origin_country,
    longitude, latitude, baro_altitude, on_ground,
    velocity, true_track, vertical_rate,
    geo_altitude, squawk, spi, position_source
)
SELECT
    fs.snapshot_id,
    trim(state->>0),
    trim(state->>1),
    state->>2,
    (state->>5)::numeric,
    (state->>6)::numeric,
    (state->>7)::numeric,
    (state->>8)::boolean,
    (state->>9)::numeric,
    (state->>10)::numeric,
    (state->>11)::numeric,
    (state->>13)::numeric,
    state->>14,
    (state->>15)::boolean,
    (state->>16)::smallint
FROM flight_json_data f
JOIN flight_snapshots fs
    ON to_timestamp((f.raw_json->>'time')::bigint) = fs.snapshot_time
CROSS JOIN jsonb_array_elements(f.raw_json->'states') AS state
WHERE f.raw_json->'states' IS NOT NULL
  AND f.raw_json->>'states' != 'null';

COMMIT;
