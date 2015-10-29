-- NOTE: If adding indexes to existing tables, look for "setval" below (users, devices and device_data),
--       uncomment and set the numbers to match your latest id values. 

CREATE EXTENSION postgis;
CREATE TYPE activity_type_enum AS ENUM ('IN_VEHICLE', 'ON_BICYCLE', 'ON_FOOT', 'RUNNING', 'STILL', 'TILTING', 'UNKNOWN', 'WALKING');

-- ********** USERS TABLE START ******************

--
-- Name: users_id_seq; Type: SEQUENCE; Schema: public; Owner: regularroutes
--

CREATE SEQUENCE users_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE users_id_seq OWNER TO regularroutes;

--
-- Name: users_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: regularroutes
--

ALTER SEQUENCE users_id_seq OWNED BY users.id;

--
-- Name: id; Type: DEFAULT; Schema: public; Owner: regularroutes
--

ALTER TABLE ONLY users ALTER COLUMN id SET DEFAULT nextval('users_id_seq'::regclass);

--
-- Name: users_id_seq; Type: SEQUENCE SET; Schema: public; Owner: regularroutes
--

-- NOTE!! If running this over an existing database, uncomment and set the number below to *match*
-- the current last id in your users table!!!

-- SELECT pg_catalog.setval('users_id_seq', 12, true);


--
-- Name: users_pkey; Type: CONSTRAINT; Schema: public; Owner: regularroutes; Tablespace: 
--

ALTER TABLE ONLY users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: users_user_id_key; Type: CONSTRAINT; Schema: public; Owner: regularroutes; Tablespace: 
--

ALTER TABLE ONLY users
    ADD CONSTRAINT users_user_id_key UNIQUE (user_id);


--
-- Name: idx_users_user_id; Type: INDEX; Schema: public; Owner: regularroutes; Tablespace: 
--

CREATE INDEX idx_users_user_id ON users USING btree (user_id);

-- ********** DEVICES TABLE START ******************

--
-- Name: devices_id_seq; Type: SEQUENCE; Schema: public; Owner: regularroutes
--

CREATE SEQUENCE devices_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE devices_id_seq OWNER TO regularroutes;

--
-- Name: devices_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: regularroutes
--

ALTER SEQUENCE devices_id_seq OWNED BY devices.id;


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: regularroutes
--

ALTER TABLE ONLY devices ALTER COLUMN id SET DEFAULT nextval('devices_id_seq'::regclass);

--
-- Name: devices_id_seq; Type: SEQUENCE SET; Schema: public; Owner: regularroutes
--

-- NOTE!! If running this over an existing database, uncomment and set the number below to *match*
-- the current last id in your devices table!!!

-- SELECT pg_catalog.setval('devices_id_seq', 85, true);


--
-- Name: devices_pkey; Type: CONSTRAINT; Schema: public; Owner: regularroutes; Tablespace: 
--

ALTER TABLE ONLY devices
    ADD CONSTRAINT devices_pkey PRIMARY KEY (id);


--
-- Name: devices_token_key; Type: CONSTRAINT; Schema: public; Owner: regularroutes; Tablespace: 
--

ALTER TABLE ONLY devices
    ADD CONSTRAINT devices_token_key UNIQUE (token);


--
-- Name: uix_device_id_installation_id; Type: CONSTRAINT; Schema: public; Owner: regularroutes; Tablespace: 
--

ALTER TABLE ONLY devices
    ADD CONSTRAINT uix_device_id_installation_id UNIQUE (device_id, installation_id);


--
-- Name: uix_token; Type: CONSTRAINT; Schema: public; Owner: regularroutes; Tablespace: 
--

ALTER TABLE ONLY devices
    ADD CONSTRAINT uix_token UNIQUE (token);


--
-- Name: fk_user_id; Type: FK CONSTRAINT; Schema: public; Owner: regularroutes
--

ALTER TABLE ONLY devices
    ADD CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES users(id) MATCH FULL;

-- ********** DEVICE-DATA TABLE START ******************

--
-- Name: device_data_id_seq; Type: SEQUENCE; Schema: public; Owner: regularroutes
--

CREATE SEQUENCE device_data_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE device_data_id_seq OWNER TO regularroutes;

--
-- Name: device_data_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: regularroutes
--

ALTER SEQUENCE device_data_id_seq OWNED BY device_data.id;


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: regularroutes
--

ALTER TABLE ONLY device_data ALTER COLUMN id SET DEFAULT nextval('device_data_id_seq'::regclass);

--
-- Name: device_data_id_seq; Type: SEQUENCE SET; Schema: public; Owner: regularroutes
--


-- NOTE!! If running this over an existing database, uncomment and set the number below to *match*
-- the current last id in your device_data table!!!

-- SELECT pg_catalog.setval('device_data_id_seq', 10751928, true);


--
-- Name: device_data_pkey; Type: CONSTRAINT; Schema: public; Owner: regularroutes; Tablespace: 
--

ALTER TABLE ONLY device_data
    ADD CONSTRAINT device_data_pkey PRIMARY KEY (id);


--
-- Name: idx_device_data_device_id_time; Type: INDEX; Schema: public; Owner: regularroutes; Tablespace: 
--

CREATE INDEX idx_device_data_device_id_time ON device_data USING btree (device_id, "time");


--
-- Name: idx_device_data_snapping_time_null; Type: INDEX; Schema: public; Owner: regularroutes; Tablespace: 
--

CREATE INDEX idx_device_data_snapping_time_null ON device_data USING btree (snapping_time) WHERE (snapping_time IS NULL);


--
-- Name: idx_device_data_time; Type: INDEX; Schema: public; Owner: regularroutes; Tablespace: 
--

CREATE INDEX idx_device_data_time ON device_data USING btree ("time");


--
-- Name: device_data_device_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: regularroutes
--

ALTER TABLE ONLY device_data
    ADD CONSTRAINT device_data_device_id_fkey FOREIGN KEY (device_id) REFERENCES devices(id);


