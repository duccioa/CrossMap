from matplotlib import use
use('TkAgg')
import osmnx as ox
idf = ox.graph_from_place('Ile de France, France')
ox.save_graphml(idf, filename='idf_road_network.graphml', folder='/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/05_Data/OpenStreetMap')
ox.save_graph_shapefile(idf, filename='idf_road_network-shape', folder='/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/05_Data/OpenStreetMap')


