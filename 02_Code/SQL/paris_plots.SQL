ALTER TABLE paris.plots 
    ADD COLUMN far NUMERIC(4,1),
    ADD COLUMN gis NUMERIC(4,3),
    ADD COLUMN building_height INT,
    ADD COLUMN building_compactness NUMERIC(4,3), -- Polsby-Popper on ST_ConvexHull https://www.azavea.com/blog/2016/07/11/measuring-district-compactness-postgis/    
    ADD COLUMN plot_compactness DOUBLE PRECISION