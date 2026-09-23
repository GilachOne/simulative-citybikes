CREATE TABLE IF NOT EXISTS gilach_networks (
    id text PRIMARY KEY,
    name text,
    location jsonb,
    href text,
    company jsonb,
    gbfs_href text,
    system text,
    source text,
    ebikes boolean,
    scooters boolean,
    instances jsonb,
    license jsonb,
    raw_payload jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS gilach_stations (
    station_id text NOT NULL,
    network_id text NOT NULL REFERENCES gilach_networks(id),
    name text,
    latitude double precision CHECK (latitude BETWEEN -90 AND 90),
    longitude double precision CHECK (longitude BETWEEN -180 AND 180),
    timestamp timestamptz NOT NULL,
    free_bikes integer CHECK (free_bikes >= 0),
    empty_slots integer CHECK (empty_slots >= 0),
    extra jsonb,
    raw_payload jsonb NOT NULL,
    PRIMARY KEY (network_id, station_id)
);
