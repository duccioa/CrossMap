import logging
import json
from osgeo import gdal
from osgeo import ogr

gdal.SetConfigOption("PG_LIST_ALL_TABLES", "YES")

def get_pg_layers(connString):
    conn = ogr.Open(connString)
    layerList = []
    for i in conn:
        daLayer = i.GetName()
        if daLayer not in layerList:
            layerList.append(daLayer)

    layerList.sort()

    for j in layerList:
        print(j)
    conn.Destroy()

    return layerList

def GetPGLayerFieldTypes( lyr_name, connString ):
    conn = ogr.Open(connString)

    lyr = conn.GetLayer( lyr_name )
    if lyr is None:
        print("lyr is None")

    lyrDefn = lyr.GetLayerDefn()
    for i in range( lyrDefn.GetFieldCount() ):
        fieldName =  lyrDefn.GetFieldDefn(i).GetName()
        fieldTypeCode = lyrDefn.GetFieldDefn(i).GetType()
        fieldType = lyrDefn.GetFieldDefn(i).GetFieldTypeName(fieldTypeCode)
        fieldWidth = lyrDefn.GetFieldDefn(i).GetWidth()
        GetPrecision = lyrDefn.GetFieldDefn(i).GetPrecision()

        print(fieldName + " - " + fieldType+ " " + str(fieldWidth) + " " + str(GetPrecision))

    conn.Destroy()

# Connect to postgreSQL database
logger = logging.getLogger('PostgreSQL to graph')
credential_file_path = '/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/04_Admin/03_Credentials/crossmapDB_credentials.json'
with open(credential_file_path) as data_file:
    credential_json = json.load(data_file)

credentials = credential_json['crossmap_database_credentials']['localhost']
host = credentials['host']
port = credentials['port']
database = credentials['database']
user = credentials['user']
password = credentials['password']
driver = credentials['driver']
connection_string = f"PG: host={host} dbname={database} user={user} password={password}"

layer_list = get_pg_layers(connection_string)

GetPGLayerFieldTypes('paris.osm_line_idf_view', connection_string)

conn = ogr.Open(connection_string)
osm_line_view = conn['paris.osm_line_idf_view']
schemas = osm_line_view.schema
i=0
for schema in schemas:
    print(i, ": ", schema.GetName())
    i += 1

# From connection to the database, extract a table
x = conn.ExecuteSQL("SELECT * FROM paris.osm_line_idf_view where highway in ('residential', 'unclassified', 'tertiary', 'primary', 'secondary', 'steps', 'pedestrian', 'living_street', 'motorway', 'primary_link', 'trunk', 'secondary_link', 'road', 'contruction', 'tertiary_link')")
# Extract row 0 of the table
fields = ['name', 'osm_id']
f = x[0]



gt = []
for i in range(0, 100):
    row = x[i]
    g = row.geometry()
    geom_type = g.GetGeometryType()
    print(f"{i}:\t{geom_type}")
    gt.append(geom_type)

gt.unique()

row = x[0]
name = row['name']
osm_id = row['osm_id']
g = row.geometry()

attrs = {"name":name, "osm_id":osm_id}
edges = edges_from_line(g, attrs, simplify=False, geom_attrs=True, export_geom='Wkt')
for edge in edges:
    print(edge)