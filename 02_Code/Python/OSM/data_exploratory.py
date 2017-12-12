import pandas as pd
import os
import json
import sqlalchemy
import logging
"""
Reduce data size for Carto and export it to geojson
"""
# Path variables
logger = logging.getLogger('Crossmap - INSEE to geojson')

target_folder = "/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/05_Data/OSM/"

credential_file_path = '/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/04_Admin/03_Credentials/crossmapDB_credentials.json'

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

osm_lines = pd.read_sql("""
    SELECT * FROM osm.planet_osm_line LIMIT 10000;""", engine)