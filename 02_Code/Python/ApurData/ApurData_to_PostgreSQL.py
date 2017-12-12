import os
import json
import sqlalchemy
import logging
import subprocess
"""
Reduce data size for Carto and export it to geojson
"""
# Path variables
logger = logging.getLogger('Crossmap - Apur to PostgreSQL')

target_folder = "/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/05_Data/Apur/"
parcelle_cadastrale_file = "PARCELLE_CADASTRALE/PARCELLE_CADASTRALE.shp"
parcelle_path = os.path.join(target_folder, parcelle_cadastrale_file)
credential_file_path = '/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/04_Admin/03_Credentials/crossmapDB_credentials.json'

schema_name = 'paris'
table_name = 'plots'
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

#password = input("Insert password:\n> ")

shp_to_postgresql = f"shp2pgsql -I -s 2154 {parcelle_cadastrale_file} {schema_name}.{table_name} -expolodecollections | psql --host {host} --port {port} -d {database} -U {user}" #-W {password}

subprocess.call(shp_to_postgresql, shell=True)


