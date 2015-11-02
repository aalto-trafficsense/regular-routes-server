SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;

CREATE EXTENSION postgis;
CREATE TYPE activity_type_enum AS ENUM ('IN_VEHICLE', 'ON_BICYCLE', 'ON_FOOT', 'RUNNING', 'STILL', 'TILTING', 'UNKNOWN', 'WALKING');

--
-- Name: users; Type: TABLE; Schema: public; Owner: regularroutes; Tablespace: 
--

CREATE TABLE users (
    id integer NOT NULL,
    user_id character varying NOT NULL,
    google_refresh_token character varying,
    google_server_access_token character varying,
    register_timestamp timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE users OWNER TO regularroutes;


--
-- Name: devices; Type: TABLE; Schema: public; Owner: regularroutes; Tablespace: 
--

CREATE TABLE devices (
    id integer NOT NULL,
    token uuid NOT NULL,
    created timestamp without time zone DEFAULT now() NOT NULL,
    last_activity timestamp without time zone DEFAULT now() NOT NULL,
    user_id integer NOT NULL,
    device_id character varying(128) NOT NULL,
    installation_id uuid NOT NULL,
    device_model character varying(128) DEFAULT '(unknown)'::character varying
);


ALTER TABLE devices OWNER TO regularroutes;


--
-- Name: device_data; Type: TABLE; Schema: public; Owner: regularroutes; Tablespace: 
--

CREATE TABLE device_data (
    id integer NOT NULL,
    device_id integer NOT NULL,
    coordinate geography(Point,4326) NOT NULL,
    accuracy double precision NOT NULL,
    "time" timestamp without time zone NOT NULL,
    activity_1 activity_type_enum,
    activity_1_conf integer,
    activity_2 activity_type_enum,
    activity_2_conf integer,
    activity_3 activity_type_enum,
    activity_3_conf integer,
    waypoint_id bigint,
    snapping_time timestamp without time zone
);


ALTER TABLE device_data OWNER TO regularroutes;

