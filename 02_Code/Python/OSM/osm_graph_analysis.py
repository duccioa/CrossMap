import osmnx as ox
import networkx as nx

import re
import time
import os
import ast
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely.geometry import LineString
from shapely import wkt

from networkx.utils.misc import make_str

ox.config(log_file=True, log_console=True, use_cache=True)


def graph_head(G, n=5):
    i = 0
    for e in G.edges(data=True):
        if i < n:
            print(e)
            i += 1
        else:
            break


idf_bak = ox.load_graphml('./data/idf_road_network.graphml')
idf = idf_bak

osm_filters = ['residential', 'unclassified', 'tertiary', 'primary', 'secondary', 'steps', 'pedestrian', 'living_street', 'motorway', 'primary_link', 'trunk', 'secondary_link', 'road', 'contruction', 'tertiary_link']

b_stats = ox.basic_stats(idf)
e_stats = ox.extended_stats(idf)



highway = []
for u, v, d in idf.edges(data=True):
    highway.append(d.get('highway'))
highway_unique = set()
for l in highway:
    print(l)
print(idf.number_of_nodes())
print(idf.number_of_edges())

idf_e = [(u, v, d) for u, v, d in idf.edges(data=True) if d['highway'] in osm_filters]
idf_sel = nx.MultiDiGraph()
# Multigraph with filtered roads
idf_sel.add_edges_from(idf_e)
print(idf_sel.number_of_nodes())
print(idf_sel.number_of_edges())
# Undirected subset graph
ox.save_graphml(idf_sel, filename='idf_road_network_sub.graphml', folder='/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/05_Data/OpenStreetMap')
idf_u = nx.Graph(idf_sel)
idf_u
graph_head(idf_u)

# Calculate approximate betweenneess
b10 = nx.edge_betweenness_centrality(idf_u, normalized=False, weight='length', k=10)
b100 = nx.edge_betweenness_centrality(idf_u, normalized=False, weight='length', k=100)

for k, v in b10.items():
    u, v = k
    idf_u[u][v]['b10'] = v

for k, v in b100.items():
    u, v = k
    idf_u[u][v]['b100'] = v

# Undirected subset graph with betweenness
filename = 'idf_road_network_sub_attr.graphml'
folder = '/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/05_Data/OpenStreetMap'
G_save = idf_u.copy()
for dict_key in G_save.graph:
    # convert all the graph attribute values to strings
    G_save.graph[dict_key] = make_str(G_save.graph[dict_key])
for _, data in G_save.nodes(data=True):
    for dict_key in data:
        print(dict_key)
        # convert all the node attribute values to strings
        data[dict_key] = make_str(data[dict_key])
for _, _, data in G_save.edges(data=True):  # keys=False,
    for dict_key in data:
        # convert all the edge attribute values to strings
        data[dict_key] = make_str(data[dict_key])

nx.write_graphml(G_save, '{}/{}'.format(folder, filename))
del(G_save)


graph_head(idf_u)

edata = idf_u[123023][3608763243]
geom = edata['geometry']

g_load_test = ox.save_load.load_graphml(filename, folder)
graph_head(g_load_test)
del(g_load_test)

