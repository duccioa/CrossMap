import osmnx as ox
idf = ox.graph_from_place('Ile de France, France')
ox.plot_graph(idf, node_size = 0.5, edge_linewidth=0.2)
b_stats = ox.basic_stats(idf)
e_stats = ox.extended_stats(idf)
