-- Drop existing tables
DROP TABLE IF EXISTS event_tag CASCADE;
DROP TABLE IF EXISTS event_source CASCADE;
DROP TABLE IF EXISTS event_location CASCADE;
DROP TABLE IF EXISTS location_coordinates CASCADE;
DROP TABLE IF EXISTS tags CASCADE;
DROP TABLE IF EXISTS sources CASCADE;
DROP TABLE IF EXISTS events CASCADE;
DROP TABLE IF EXISTS locations CASCADE;
DROP TABLE IF EXISTS coordinates CASCADE;

-- ======================
-- MAIN TABLES
-- ======================

CREATE TABLE events (
    event_id SERIAL PRIMARY KEY,
	external_record_id INTEGER NOT NULL UNIQUE,
    event_name VARCHAR(255) NOT NULL,
    start_date_int INTEGER NOT NULL,
    end_date_int INTEGER,
    date_description VARCHAR(150) NOT NULL,
    precise_date BOOLEAN NOT NULL,
    event_description TEXT,
    contributor_name VARCHAR(150) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP
);

CREATE TABLE tags (
    tag_id SERIAL PRIMARY KEY,
    tag_name VARCHAR(150) NOT NULL UNIQUE
);

CREATE TABLE sources (
    source_id SERIAL PRIMARY KEY,
    source_type VARCHAR(50) NOT NULL CHECK (source_type IN ('url','book','article','other')),
    source_value TEXT NOT NULL UNIQUE
);

--create geometry_type ENUM
CREATE TYPE geometry_type_enum AS ENUM ('point', 'line', 'polygon');

CREATE TABLE locations (
    location_id SERIAL PRIMARY KEY,
    location_description VARCHAR(255) NOT NULL UNIQUE,
    geometry_type geometry_type_enum NOT NULL
);

CREATE TABLE coordinates (
    coordinates_id SERIAL PRIMARY KEY,
    longitude NUMERIC(10,6) NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    latitude NUMERIC(10,6) NOT NULL CHECK (latitude BETWEEN -90 AND 90),
	CONSTRAINT unique_lat_lon UNIQUE (latitude, longitude)
);

-- ======================
-- RELATIONSHIP TABLES
-- ======================

CREATE TABLE event_tag (
    event_id INTEGER NOT NULL,
    tag_id INTEGER NOT NULL,
    PRIMARY KEY (event_id, tag_id),
    FOREIGN KEY (event_id) REFERENCES events(event_id) ON DELETE CASCADE,
    FOREIGN KEY (tag_id) REFERENCES tags(tag_id) ON DELETE CASCADE
);

CREATE TABLE event_source (
    event_id INTEGER NOT NULL,
    source_id INTEGER NOT NULL,
    PRIMARY KEY (event_id, source_id),
    FOREIGN KEY (event_id) REFERENCES events(event_id) ON DELETE CASCADE,
    FOREIGN KEY (source_id) REFERENCES sources(source_id)
);

CREATE TABLE event_location (
    event_id INTEGER NOT NULL,
    location_id INTEGER NOT NULL,
    precise_location BOOLEAN NOT NULL,
    PRIMARY KEY (event_id, location_id),
    FOREIGN KEY (event_id) REFERENCES events(event_id) ON DELETE CASCADE,
    FOREIGN KEY (location_id) REFERENCES locations(location_id)
);

CREATE TABLE location_coordinates (
    location_id INT NOT NULL,
    coordinates_id INT NOT NULL,
    point_number SMALLINT NOT NULL,
    PRIMARY KEY (location_id, coordinates_id, point_number),
    FOREIGN KEY (location_id) REFERENCES locations(location_id) ON DELETE CASCADE,
    FOREIGN KEY (coordinates_id) REFERENCES coordinates(coordinates_id)
);
