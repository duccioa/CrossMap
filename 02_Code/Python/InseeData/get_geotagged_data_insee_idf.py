from sys import argv
import urllib.request
import pandas as pd
from os import remove
import string
import random

"""
Get data from the geotagged INSEE database, merge it and save it to a csv
ex.:
python3 get_geotagged_data_insee_idf.py http://212.47.238.202/geo_sirene/last/ /Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/05_Data/INSEE/ False
"""
script, url, target_folder, download_files = argv

# target_folder = '/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/05_Data/INSEE/'

# url = 'http://212.47.238.202/geo_sirene/last/'

# download_files = 'False'

pc_ile_de_france = {'Essonne': '91',
                    'Hauts-de-Seine': '92',
                    'Seine-et-Marne': '77',
                    'Seine-Saint-Denis': '93',
                    "Val-d\'Oise": '95',
                    'Paris': '75',
                    'Val-de-Marne': '94',
                    'Yvelines': '78'
                    }

# ***************** Read zip files and merge them into a csv ***********************************
# Create file names and download the files
files = []
for k, v in pc_ile_de_france.items():
    if v != '75':
        file_name = 'geo-sirene_' + v + '.csv.7z'
        file_url = url + file_name
        file_path = target_folder + file_name
        t = (file_url, file_path)
        files.append(t)
    else:
        for i in range(1,10):
            i = str(i)
            file_name = 'geo-sirene_7510' + i + '.csv.7z'
            file_url = url + file_name
            file_path = target_folder + file_name
            t = (file_url, file_path)
            files.append(t)
        for i in range(11,21):
            i = str(i)
            file_name = 'geo-sirene_751' + i + '.csv.7z'
            file_url = url + file_name
            file_path = target_folder + file_name
            t = (file_url, file_path)
            files.append(t)

if download_files == 'True':
    for file in files:
        print(f"Downloading from url {file[0]} ...")
        urllib.request.urlretrieve(file[0], file[1])

input("Please unzip the files manually, press ENTER when ready...")

csv_names = []
for zip_file in files:
    csv_names.append(zip_file[1].replace(".7z", ""))

df = pd.DataFrame()
for csv in csv_names:
    print(f"Reading '{csv}'...")
    df = df.append(pd.read_csv(csv, low_memory=False))

df.index.name = 'id'
df.set_index([list(df['SIREN'].astype(str) + '-' + df['NIC'].astype(str))], inplace=True)
line_num = 0
print(f"\nPrinting line {line_num}")
for column in df.columns:
    print(column, ": ", df[column][line_num])
# Adjust dates
print("\nConverting columns to datetime...")
print("Column 'AMINTREN'")
df['AMINTREN'] = pd.to_datetime(df['AMINTREN'].astype(str) + '01', errors='coerce')
print("Column 'DCREN'")
df['DCREN'] = pd.to_datetime(df['DCREN'].astype(str).str[:-2], errors='coerce')
print("Column 'DEFEN'")
df['DEFEN'] = pd.to_datetime(df['DEFEN'].astype(str).str[:-2] + '0101', errors='coerce')
print("Column 'DAPEN'")
df['DAPEN'] = pd.to_datetime(df['DAPEN'].astype(str) + '0101', errors='coerce')
print("Column 'DCRET'")
df['DCRET'] = pd.to_datetime(df['DCRET'].astype(str).str[:-2], errors='coerce')
print("Column 'DDEBACT'")
df['DDEBACT'] = pd.to_datetime(df['DDEBACT'].astype(str), errors='coerce')
print("Column 'DEFET'")
df['DEFET'] = pd.to_datetime(df['DEFET'].astype(str).str[:-2] + '0101', errors='coerce')
print("Column 'DAPET'")
df['DAPET'] = pd.to_datetime(df['DAPET'].astype(str).str[:-2] + '0101', errors='coerce')
print("Column 'AMINTRET'")
df['AMINTRET'] = pd.to_datetime(df['AMINTRET'].astype(str) + '01', errors='coerce')

#save
print("\nSaving to csv...")
df.to_csv(target_folder + 'insee_ile_de_france.csv.zip', compression='gzip', index=True, index_label='id')
print("\nDeleting csv files...")
for csv_file in csv_names:
    remove(csv_file)
print("\n\tDone")

