-- OSM
--Create database
CREATE INDEX idx_planet_osm_point_tags ON osm.planet_osm_point USING gist(tags);
CREATE INDEX idx_planet_osm_polygon_tags ON osm.planet_osm_polygon USING gist(tags);
CREATE INDEX idx_planet_osm_line_tags ON osm.planet_osm_line USING gist(tags);
INSERT INTO paris.boundaries SELECT st_union(geom) AS geom, 'Ile de France' AS commune FROM paris.communes;
--explore
SELECT cutting FROM osm.planet_osm_polygon;


--OSM : Ile de France streets
BEGIN;
CREATE MATERIALIZED VIEW paris.osm_line_idf_view AS(
    SELECT t1.*, st_transform(t1.way,2154) AS geom_2154
    FROM osm.planet_osm_line AS t1 
    JOIN paris.boundaries AS t2
    ON ST_Intersects(t1.way, st_transform(t2.geom_2154, 3857))
    WHERE t2.commune='Ile de France'
    );
COMMIT;

SELECT AddGeometryColumn('osm', 'planet_osm_polygon', 'geom_centroids', 3857, 'POINT', 2, FALSE);

BEGIN;
UPDATE osm.planet_osm_polygon SET geom_centroids = st_centroid(way);
COMMIT;
BEGIN;
CREATE INDEX ON osm.planet_osm_polygon USING gist ("geom_centroids");
COMMIT;

BEGIN;
CREATE MATERIALIZED VIEW paris.osm_polygon_idf_view AS(
    SELECT t1.*, st_transform(t1.way,2154) AS geom_2154
    FROM osm.planet_osm_polygon AS t1 
    JOIN paris.boundaries AS t2
    ON ST_Intersects(t1.geom_centroids, st_transform(t2.geom_2154, 3857))
    WHERE t2.commune='Ile de France'
    );
COMMIT;


