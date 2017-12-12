import pandas as pd
import os
import json
import sqlalchemy
import logging
import subprocess
"""
Reduce data size for Carto
"""
# Path variables
logger = logging.getLogger('Crossmap - INSEE to SQL')

target_folder = "/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/05_Data/INSEE"
file_name = "insee_ile_de_france_toSQL.csv.zip"
column_file_name = "insee_Siren_Data_fields.csv"
credential_file_path = '/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/04_Admin/03_Credentials/crossmapDB_credentials.json'
cp2ci_path = "/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/05_Data/INSEE/correspondances-code-insee-code-postal.csv"
data_file_path = os.path.join(target_folder, file_name)
column_file_path = os.path.join(target_folder, column_file_name)
# SQL connection
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


cp_outer_ring = pd.read_sql("select t1.code_postal from paris.communes as t1 join paris.boundaries as t2 on st_intersects(t1.geom, st_buffer(t2.geom, 0.01)) and t1.code_departement!='75';", engine)
cp_outer_ring.replace({'94100/94210': '94210', '93200/93210': '93210'}, inplace=True)
outer_list = []
for commune in cp_outer_ring['code_postal']:
    outer_list.append(commune)
for i in range(1,21):
    cp = '750' + '{:02}'.format(i)
    outer_list.append(cp)

table_name = "insee_paris_plus"
schema_name = "paris"

df = pd.read_sql(f"SELECT * FROM {schema_name}.{table_name} WHERE ")
df['id'] = df.index
df.set_index('codpos', inplace=True)
df_paris_plus = df.loc[outer_list]
df_paris_plus.reset_index(inplace=True)

print(df_paris_plus.shape)
for col in df_paris_plus.columns:
    print(f"{col} nulls: {df_paris_plus[col].isnull().sum()}")
df_paris_plus.dropna(subset=['apet700', 'depet', 'tefet', 'nomen_long', 'apen700', 'dcret', 'longitude', 'latitude', 'id'], inplace=True)
print(df_paris_plus.shape)
df_paris_plus.drop(['tca', 'natetab', 'efetcent', 'activnat'], axis=1, inplace=True)
df_paris_plus.to_csv(target_folder + "insee_paris_plus.csv.zip", compression='gzip', index_label=False)


# Create geojson
command_line = 'ogr2ogr -f GeoJSON insee_paris_plus.geojson insee_paris_plus.csv -oo X_POSSIBLE_NAMES=longitude -oo Y_POSSIBLE_NAMES=latitude'
proc = subprocess.Popen([command_line], stdout=subprocess.PIPE, shell=True)
output = proc.stdout.read()
d = eval(output)


with open(target_folder + 'insee_paris_plus_clean.geojson', 'w+') as output_geojson, open(target_folder + 'insee_paris_plus.geojson', 'r') as input_geojson:


    def remove_latlon_info(d):
        if not isinstance(d, (dict, list)):
            return d
        if isinstance(d, list):
            return [remove_latlon_info(v) for v in d]
        return {k: remove_latlon_info(v) for k, v in d.items()
                if k not in {'longitude', 'latitude'}}


    print("Load geojson.")
    geo_data = json.loads(input_geojson.read())
    print("Remove columns.")
    geo_data = remove_latlon_info(geo_data)
    print("Save geojson to file.")
    json.dump(geo_data, output_geojson, ensure_ascii=False, separators=(',', ':'))


df_paris_plus['apen700_short'] = df_paris_plus['apen700'].str[:-3] + '.' + df_paris_plus['apen700'].str[-3:-2]
print(df_paris_plus['apen700_short'].value_counts())

apen_descr = pd.read_csv(target_folder + "apen700_labels.csv")
apen_descr.dropna(axis=0, inplace=True)
print(apen_descr.shape)

df_merge = pd.merge(df_paris_plus, apen_descr, left_on='apen700_short', right_on='Code')
print(df_merge.head())
print(df_merge['Description'].value_counts())

df_to_carto = df_merge[df_merge['apen700_short'] != '68.2']
columns = list(df_to_carto.columns)

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
            "CODPOS": "%s",
            "DEPET": "%s",
            "TEFET": "%s",
            "DCRET": "%s",
            "NOMEN_LONG": "%s",
            "EFENCENT": "%s",
            "DCREN": "%s",
            "APEN700_short": "%s",
            "Description": "%s"
            }
            },
'''


for id, row in df_to_carto[:4].iterrows():

    output += template % (row['longitude'],
                              row['latitude'],
                              row['codpos'],
                              row['depet'],
                              row['tefet'],
                              row['dcret'],
                              row['nomen_long'],
                              row['efencent'],
                              row['dcren'],
                              row['apen700_short'],
                              row['Description']
                              )

output = output[:-2]+ \
    ''' \
    }]}
    '''



with open(target_folder + "df_to_carto_test.json", "w") as file:
    file.write(output)




### Nothing to do with the project
