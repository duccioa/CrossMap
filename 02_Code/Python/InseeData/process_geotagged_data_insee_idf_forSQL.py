import pandas as pd
import logging
from numpy import nan
import json
import sqlalchemy
import psycopg2

"""
Load the merged INSEE Ile de France dataset, select relevant columns and write it to a csv.
Ex.:
python3 process_geotagged_data_insee_idf_forSQL.py /Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/05_Data/INSEE/ insee_ile_de_france.csv.zip
"""
target_folder = '/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/05_Data/INSEE/'
file_name = 'insee_ile_de_france.csv.zip'
credential_file_path = '/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/04_Admin/03_Credentials/crossmapDB_credentials.json'
save_to_SQL_file = "create_insee_idf_table.SQL"

logger = logging.getLogger('Crossmap - INSEE to SQL')
# Read the INSEE csv
# The file "insee_Siren_Data_fields.csv" controls the columns to be read and their format
df_columns = pd.read_csv(target_folder + 'insee_Siren_Data_fields.csv')
df_dtypes = {}
for i, row in df_columns.iterrows():
    df_dtypes[row['Label']] = row['Python Type']
datetime_columns = list(df_columns[(df_columns['SQL Type'] == 'timestamp') & (df_columns['Selection_to_postgresql'])]['Label'])


print(f"Reading {file_name}")
df = pd.read_csv(target_folder + file_name, dtype=df_dtypes, parse_dates=datetime_columns,
                 compression='gzip', index_col='id')
print("Overview of the dataset:\n")
for col in df.columns:
    print(f"{col}: {df[col][0]}")
print("\nSelect relevant columns")
selected_columns = df_columns[df_columns['Selection_to_postgresql']]['Label']
selection = []
for col in selected_columns:
    if col in df.columns:
        selection.append(col)

df = df[selection]
df.columns = map(str.lower, df.columns)
df.dropna(subset=['nic', 'siren'], inplace=True)
# Clean the data
replacements = {'efetcent': {'NN': nan},
                'natetab':{'1.0': '1', '2.0':'2', '3.0': '3', '4.0': '4', '5.0': '5', '6.0': '6', '9.0': '9'},
                'activnat': {'NR': nan, '01': '14', '02': '14'}}
df.replace(to_replace=replacements, inplace=True)

df['apen700_short'] = df['apen700'].str[:-3] + '.' + df['apen700'].str[-3:-2]
df['apen700_vshort'] = df['apen700'].str[:-3]
print(df['apen700_short'].value_counts())
print(df['apen700_vshort'].value_counts())



apen_descr = pd.read_csv(target_folder + "apen700_labels.csv")
apen_descr.dropna(axis=0, inplace=True)
print(apen_descr.shape)

df_merge = pd.merge(df, apen_descr, how='outer', left_on='apen700_short', right_on='Code', copy=False)
df_merge = pd.merge(df_merge, apen_descr, how='outer', left_on='apen700_vshort', right_on='Code', copy=False)
df_merge.drop(['Code_x', 'Code_y'], axis=1, inplace=True)
df_merge.rename(columns={'Description_x': 'apen700_des_long', 'Description_y': 'apen700_des_short'}, inplace=True)
print(df_merge.head())
print(df_merge['apen700_des_short'].value_counts())
print(df_merge['apen700_des_long'].value_counts())

df_merge['id'] = df_merge['siren'] + '-' + df_merge['nic']
# Save the results
choice = input("Do you want to save the results?\ny/n > ")
if choice == 'y':
    print(f"Saving file to {file_name.split('.')[0]}" + "_toSQL.csv.zip")
    df.to_csv(target_folder + file_name.split('.')[0] + "_toSQL.csv.zip", index=True, compression='gzip', index_label='id')
    print("Done")
else:
    print("Not saving the file, continue...")

# Upload to SQL database
with open(credential_file_path) as data_file:
    credential_json = json.load(data_file)


credentials = credential_json['crossmap_database_credentials']
servers = ""
for k, v in credentials.items():
    servers = k + ", " + servers

print("What database do you wish to upload the data to? (type 'q' for quitting)")
choice = input(f'{servers} >')
if choice not in servers:
    print("Please select a server or quit (q).")
    choice = input(f'{servers} >')
elif choice == 'q':
    exit("Exit without uploading.")
else:
    credentials = credential_json['crossmap_database_credentials'][choice]

host = credentials['host']
port = credentials['port']
database = credentials['database']
user = credentials['user']
password = credentials['password']
driver = credentials['driver']
sql_connection = driver + "://" + user + ":" + password + "@" + host + ":" + port + "/" + database
print(f"Creating connection on {host} with database {database} for user {user}")
engine = sqlalchemy.create_engine(sql_connection)


# Create SQL statement
df_columns['Label'] = df_columns['Label'].str.lower()
df_columns.set_index('Label', inplace=True)
schema_name = 'paris'
table_name = 'insee_idf'
create_table_command = f"""CREATE TABLE {schema_name}.{table_name} (\n
\tid CHAR(15) CONSTRAINT firstkey PRIMARY KEY,
"""
i = 0
for label in df_merge.columns:
    try:
        line = "\t" + label + " " + df_columns.loc[label]['SQL Type']
    except KeyError:
        line = "\t" + label + " TEXT"
    extra = ",\n"
    print(str(i) + ". " + line + extra)
    create_table_command = create_table_command + line + extra
    i += 1
create_table_command = create_table_command[0:-2] + ');'


try:
    conn = psycopg2.connect(f"dbname={database} user={user} host={host} password={password} port={port}")
except psycopg2.Error as e:
    logger.error('Connection failed: ' + str(e))
# Check if schema exists
cur = conn.cursor()
cur.execute(f"SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = '{schema_name}');")
if True not in cur.fetchone():
    print(f"Creating schema '{schema_name}'")
    cur.execute(f"CREATE SCHEMA {schema_name};")
# Check if the table exists
cur.execute(f"SELECT to_regclass('{schema_name}.{table_name}');")
exists = cur.fetchone()
cur.close()
if table_name not in exists:
    print(f"Creating {driver} table '{schema_name}.{table_name}' on '{host}'")
    try:
        print("Copying data into the database...")
        df_merge.to_sql(table_name, schema=schema_name, index=False, con=engine)
    except sqlalchemy.exc as e:
        logger.error('Failed to copy the data: ' + str(e))
    print(f"Indexing the table {schema_name}.{table_name}...")
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
                BEGIN;
                ALTER TABLE {schema_name}.{table_name} ADD PRIMARY KEY ("id");
                CREATE INDEX id_idx ON {schema_name}.{table_name} ("id");
                COMMIT;
                BEGIN;
                CREATE INDEX siren_idx ON {schema_name}.{table_name} ("siren");
                CREATE INDEX nic_idx ON {schema_name}.{table_name} ("nic");
                COMMIT;
                BEGIN;
                SELECT AddGeometryColumn ('{schema_name}','{table_name}','geom_point',4326,'POINT',2);
                UPDATE {schema_name}.{table_name} SET geom_point = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);
                CREATE INDEX geom_point_spidx ON {schema_name}.{table_name} USING GIST (geom_point);                
                COMMIT;
            """
        )
        cur.close()
    except psycopg2.ProgrammingError as e:
        logger.error(str(e))

else:
    try:
        print("Appending data to the database...")
        df['id'] = df.index
        df.to_sql(table_name, if_exists='append', schema=schema_name, index=False, con=engine)
    except sqlalchemy.exc as e:
        logger.error('Failed to append the data: ' + str(e))

conn.close()
print("Done.")
