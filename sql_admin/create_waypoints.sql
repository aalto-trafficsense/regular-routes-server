
--
-- Name: waypoints; Type: TABLE; Schema: public; Owner: regularroutes; Tablespace: 
--

CREATE TABLE waypoints (
    id bigint NOT NULL,
    geo geography(Point,4326) NOT NULL,
    osm_nodes bigint[] NOT NULL
);


ALTER TABLE waypoints OWNER TO regularroutes;

--
-- Name: waypoints_pkey; Type: CONSTRAINT; Schema: public; Owner: regularroutes; Tablespace: 
--

ALTER TABLE ONLY waypoints
    ADD CONSTRAINT waypoints_pkey PRIMARY KEY (id);


--
-- Name: waypoints_geo_idx; Type: INDEX; Schema: public; Owner: regularroutes; Tablespace: 
--

CREATE INDEX waypoints_geo_idx ON waypoints USING gist (geo);


--
-- Name: waypoints_osm_nodes_idx; Type: INDEX; Schema: public; Owner: regularroutes; Tablespace: 
--

CREATE INDEX waypoints_osm_nodes_idx ON waypoints USING gin (osm_nodes);

