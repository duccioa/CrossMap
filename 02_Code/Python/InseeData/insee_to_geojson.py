import pandas as pd
import os
import json
import sqlalchemy
import logging
"""
Reduce data size for Carto
"""
# Path variables
logger = logging.getLogger('Crossmap - INSEE to geojson')

target_folder = "/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/05_Data/INSEE/"
file_name = "insee_ile_de_france_toSQL.csv.zip"
column_file_name = "insee_Siren_Data_fields.csv"
credential_file_path = '/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/04_Admin/03_Credentials/crossmapDB_credentials.json'
cp2ci_path = "/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/05_Data/INSEE/correspondances-code-insee-code-postal.csv"
data_file_path = os.path.join(target_folder, file_name)
column_file_path = os.path.join(target_folder, column_file_name)
# SQL connection
with open(credential_file_path) as data_file:
    credential_json = json.load(data_file)

server = 'localhost'
credentials = credential_json['crossmap_database_credentials'][server]
host = credentials['host']
port = credentials['port']
database = credentials['database']
user = credentials['user']
password = credentials['password']
driver = credentials['driver']
sql_connection = driver + "://" + user + ":" + password + "@" + host + ":" + port + "/" + database
print(f"Creating connection on {host} with database {database} for user {user}")
engine = sqlalchemy.create_engine(sql_connection)


cp_outer_ring = pd.read_sql("select t1.code_postal from paris.communes as t1 join paris.boundaries as t2 on st_intersects(t1.geom, st_buffer(t2.geom, 0.01)) and t1.code_departement!='75';", engine)
cp_outer_ring.replace({'94100/94210': '94210', '93200/93210': '93210'}, inplace=True)
outer_list = []
for commune in cp_outer_ring['code_postal']:
    outer_list.append(commune)
for i in range(1,21):
    cp = '750' + '{:02}'.format(i)
    outer_list.append(cp)
codpos = ",".join(outer_list)

df = pd.read_sql("""
    WITH boundaries as (SELECT
        t1.code_postal, t1.geom as geom_boundaries
        FROM paris.communes as t1
        JOIN paris.boundaries as t2
        ON st_intersects( t1.geom,st_buffer( t2.geom,0.01 ) )
        OR t1.code_departement = '75'
    )
    SELECT t1.tefen, t1.dcret, t1.nomen_long, t1.apen700_vshort, t1.apen700_short, st_x(t1.geom_point) AS longitude, st_y(t1.geom_point) AS latitude FROM paris.insee_idf AS t1 join boundaries AS t2 on ST_Within(t1.geom_point, t2.geom_boundaries) where t1.apen700_short != '68.2';
""", engine)
print(df.head())
print(df.shape)
print(df.dtypes)
df['nomen_long'] = df['nomen_long'].str.replace('"', "'")

output = \
    ''' \
{ "type" : "Feature Collection",
    "features" : [ 
    '''

template = \
    '''{\
    "type" : "Feature", 
    "geometry" : {
        "type" : "Point", 
        "coordinates" : ["%s","%s"]
        },
        "properties": {
            "TEFEN": "%s",
            "DCRET": "%s",
            "NOMEN_LONG": "%s",
            "APEN700_vshort": "%s",
            "APEN700_short": "%s"
            }
            },
'''


for id, row in df.iterrows():
    output += template % (row['longitude'],
                          row['latitude'],
                          row['tefen'],
                          row['dcret'],
                          row['nomen_long'],
                          row['apen700_vshort'],
                          row['apen700_short']
                          )

output = output[:-2] + \
    ''' \
    ]}
    '''


json_file = json.loads(output)
#with open(target_folder + "df_to_carto_test.json", "w+") as file:
    #file.write(json.dumps(json_file, separators=(',', ':')))
    #file.write(output)




def df2geojson(df, save_path:str):
    import geojson
    features = []
    df.apply(lambda x:
             features.append(geojson.Feature(
                 geometry=geojson.Point(
                 (x["longitude"],
                  x["latitude"],
                  )),
                 properties=dict(tefen=x["tefen"], dcret=str(x["dcret"]), nomen_long=x["nomen_long"], apen700_vshort=x["apen700_vshort"])
             )), axis=1)

    with open(save_path, 'w+') as fp:
        geojson.dump(geojson.FeatureCollection(features), fp, sort_keys=True)

df2geojson(df[:10], target_folder + 'geojson_test.geojson')