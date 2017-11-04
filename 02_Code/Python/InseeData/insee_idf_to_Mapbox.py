#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
# python -m pip install mapbox

# convert CSV file into Shapefile using QGIS desktop programm
# convert Shapefile into GeoJSON using ogr2ogr command line tool
ogr2ogr -f GeoJSON -t_srs crs:84 insee_ile_de_france_modify.geojson insee_ile_de_france_modify.shp

# convert CSV file into GeoJSON: all fields
ogr2ogr -f GeoJSON output.geojson insee_ile_de_france.csv -oo X_POSSIBLE_NAMES=longitude -oo Y_POSSIBLE_NAMES=latitude -select id,siren,nic,l1_normalisee,l2_normalisee,l3_normalisee,l4_normalisee,l5_normalisee,l6_normalisee,l7_normalisee,codpos,cedex,depet,comet,tcd,siege,enseigne,diffcom,amintret,natetab,apet700,tefet,efetcent,origine,dcret,ddebact,activnat,lieuact,actisurf,saisonat,modet,prodet,auxilt,nomen_long,sigle,nom,prenom,civilite,nj,apen700,dapen,aprm,ess,tefen,efencent,categorie,dcren,monoact,moden,proden,esaann,tca,esaapen,esasec1n,esasec2n,esasec3n,esasec4n,vmaj,vmaj1,vmaj2,vmaj3,datemaj,geo_score,geo_adresse

# convert CSV file into GeoJSON: selected fields
ogr2ogr -f GeoJSON insee_paris_plus.geojson insee_paris_plus.csv -oo X_POSSIBLE_NAMES=longitude -oo Y_POSSIBLE_NAMES=latitude
'''

# Upload GeoJSON file into Mapbox
# python3 insee_idf_to_Mapbox.py

import os
from mapbox import Uploader
#from sys import argv

#script, file_folder, file_name = argv

os.environ["MAPBOX_ACCESS_TOKEN"] = "sk.eyJ1IjoiZHVjY2lvYSIsImEiOiJjajh4ODJrYzExZnRvMnZwbDd0eXptdnp6In0.VaBCSsTJRADWpHjE2-p3Xw"
tileset = 'duccioa.insee_idf'

# base dir & GeoJSON file
base_dir = os.path.realpath(os.path.dirname(__file__))
data_dir = os.path.join(base_dir, '/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/05_Data/INSEE/')
geojson_file = os.path.join(data_dir, 'insee_to_mapbox.geojson')


print('start uploading')

service = Uploader()
with open(geojson_file, 'rb') as src:
	upload_resp = service.upload(src, tileset)

print('uploading completed')


