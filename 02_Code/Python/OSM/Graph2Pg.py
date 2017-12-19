# Imports
from time import asctime
import sqlalchemy as sal
import networkx as nx
import json
from OSM.multiprocess_nx import edge_betweenness_centrality_parallel
from matplotlib import use
use('TkAgg')
from osmnx import graph_from_place
from osmnx.save_load import load_graphml
from OSM.graph_to_gdf import graph_to_gdf


# Function to generate WKB hex
def to_wkt(line):
    i = 1
    try:
        wkt = line.to_wkt()
    except AttributeError:
        print(f"NaN geometry: {i}")
        wkt = line
        i += 1
    return wkt


# Paths
credential_file_path = '/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/04_Admin/03_Credentials/crossmapDB_credentials.json'
filename = 'idf_road_network.graphml'
folder_path = '/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/05_Data/OpenStreetMap'
#osm_filters = ['residential', 'unclassified', 'tertiary', 'primary', 'secondary', 'steps', 'pedestrian', 'living_street', 'motorway', 'primary_link', 'trunk', 'secondary_link', 'road', 'contruction', 'tertiary_link']
print(f"{asctime()}: Loading graph {filename}")
idf = load_graphml(filename, folder_path)
print(f"{asctime()}: Done.")
#print(f"{asctime()}: Subseting graph based on osm_filters.")
#idf_e = [(u, v, d) for u, v, d in idf.edges(data=True) if d['highway'] in osm_filters]
#idf_sel = nx.MultiDiGraph()
#idf_sel.add_edges_from(idf_e)
#print(f"{asctime()}: Done.")
#DEBUG
idf = graph_from_place('Scarperia e San Piero, Italy')
print(f"{asctime()}: Creating undirected graph.")
idf_u = nx.Graph(idf)
print(f"{asctime()}: Done.")
print(f"{asctime()}: Adding crs and graph's name.")
idf_u.graph['crs'] = '4326'
idf_u.graph['name'] = 'idf_road_network'
print(f"{asctime()}: Done.")
print(f"{asctime()}: Calculating approximated inbetweenness.")
b100 = edge_betweenness_centrality_parallel(idf_u, processes=4, k=100)
#b100 = nx.edge_betweenness_centrality(idf_u, normalized=False, weight='length', k=100)
for k, v in b100.items():
    u, v = k
    idf_u[u][v]['inbetweenness_100'] = v


print(f"{asctime()}: Done.")
print(f"{asctime()}: Converting graph to geopandas dataframe.")
gdf = graph_to_gdf(idf, nodes=False, edges=True, node_geometry=False, fill_edge_geometry=True)
print(f"{asctime()}: Done.")
print(f"{asctime()}: Converting geometry column to string.")
geometry = gdf['geometry']
geom_wkt = gdf['geometry'].apply(to_wkt)
gdf['geometry'] = geom_wkt
print(f"{asctime()}: Done.")
# Convert `'geom'` column in GeoDataFrame `gdf` to hex
# Note that following this step, the GeoDataFrame is just a regular DataFrame
# because it does not have a geometry column anymore. Also note that
# it is assumed the `'geom'` column is correctly datatyped.
# gdf['geometry'] = gdf['geometry'].apply(wkb_hexer)

# PostgreSQL connection
# Variable definitions
print(f"{asctime()}: Fetching credentials and setting up variables.")
with open(credential_file_path) as data_file:
    credential_json = json.load(data_file)
credentials = credential_json['crossmap_database_credentials']['localhost']
host = credentials['host']
port = credentials['port']
database = credentials['database']
user = credentials['user']
password = credentials['password']
driver = credentials['driver']
connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
engine = sal.create_engine(connection_string)
table_name = 'road_network'
schema_name = 'paris'
print(f"{asctime()}: Done.")
# Connect to database using a context manager
print(f"{asctime()}: Copying geodataframe to PostgreSQL database {database}, in {schema_name}.{table_name}.")
with engine.connect() as conn, conn.begin():
    # Note use of regular Pandas `to_sql()` method.
    gdf.to_sql(table_name, con=conn, schema=schema_name,
               if_exists='append', index=False)
print(f"{asctime()}: Done.")
print(f"{asctime()}: Converting 'geometry' column to Geometry data type.")
with engine.connect() as conn, conn.begin():
    # Convert the `'geom'` column back to Geometry datatype, from text
    sql = f"""ALTER TABLE {schema_name}.{table_name}
               ALTER COLUMN geometry TYPE Geometry(LINESTRING, 4326)
                 USING ST_SetSRID(geometry::Geometry, 4326)"""
    conn.execute(sql)
print(f"{asctime()}: Done.")
