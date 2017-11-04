import logging
import json
import sqlalchemy
import psycopg2
import os
import pandas as pd

logger = logging.getLogger('Crossmap - INSEE to SQL')

target_folder = "/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/05_Data/INSEE"
file_name = "insee_ile_de_france_toSQL.csv.zip"
column_file_name = "insee_Siren_Data_fields.csv"
credential_file_path = '/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/04_Admin/03_Credentials/crossmapDB_credentials.json'
cp2ci_path = "/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/05_Data/INSEE/correspondances-code-insee-code-postal.csv"
data_file_path = os.path.join(target_folder, file_name)
column_file_path = os.path.join(target_folder, column_file_name)

#
with open(credential_file_path) as data_file:
    credential_json = json.load(data_file)

credentials = credential_json['crossmap_database_credentials']['localhost']
host = credentials['host']
port = credentials['port']
database = credentials['database']
user = credentials['user']
password = credentials['password']
driver = credentials['driver']
sql_connection = driver + "://" + user + ":" + password + "@" + host + ":" + port + "/" + database
print(f"Creating connection on {host} with database {database} for user {user}")
engine = sqlalchemy.create_engine(sql_connection)

cp2ci = pd.read_csv(cp2ci_path, sep=';', index_col='Code INSEE')


geom_as_text = []
for t in cp2ci['geo_shape']:
    j = json.loads(t)
    c = j['coordinates'][0]
    text = j['type'] + " (("
    for coords in c:
        text += str(coords[0]) + " " + str(coords[1]) + ","
    text = text[:-1]
    text += "))"
    geom_as_text.append(text)
cp2ci['geo_shape'] = geom_as_text

cp2ci.to_sql('communes', schema='paris', index=False, con=engine)

try:
    conn = psycopg2.connect(f"dbname={database} user={user} host={host} password={password} port={port}")
except psycopg2.Error as e:
    logger.error('Connection failed: ' + str(e))

cur = conn.cursor()
cur.execute("""
    alter table paris.communes 
        rename column "Code Postal" TO "code_postal";
    alter table paris.communes 
        rename column "Département" TO "departement";
    alter table paris.communes 
        rename column "Région" TO "region"
    alter table paris.communes 
        rename column "Altitude Moyenne" to "altitude_moyenne";
    alter table paris.communes     
        rename column "Commune" to "commune";
    alter table paris.communes     
        rename column "Statut" to "statut";
    alter table paris.communes     
        rename column "Superficie" to "superficie";
    alter table paris.communes     
        rename column "Population" to "population";
    alter table paris.communes     
        rename column "ID Geofla" to "id_geofla";
    alter table paris.communes     
        rename column "Code Commune" to "code_commune";
    alter table paris.communes     
        rename column "Code Canton" to "code_canton";
    alter table paris.communes     
        rename column "Code Arrondissement" to "code_arrondissement";
    alter table paris.communes     
        rename column "Code Département" to "code_departement";
    alter table paris.communes     
        rename column "Code Région" to "code_region";
    SELECT AddGeometryColumn ('paris','communes','geom',2154,'POLYGON',2, false);
    UPDATE paris.communes set geom=st_geomfromtext(geo_shape,2154);
    ALTER TABLE paris.communes add primary key ("id_geofla");
    create index idgeofla_idx on paris.communes ("id_geofla");
    create index geom_idx on paris.communes using gist ("geom");
""")
