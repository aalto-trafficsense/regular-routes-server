CREATE ROLE regularroutes WITH LOGIN PASSWORD 'qwerty';
CREATE EXTENSION postgis;
CREATE TYPE activity_type_enum AS ENUM ('IN_VEHICLE', 'ON_BICYCLE', 'ON_FOOT', 'RUNNING', 'STILL', 'TILTING', 'UNKNOWN', 'WALKING');

