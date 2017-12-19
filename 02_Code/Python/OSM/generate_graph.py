from graph_tool.all import *
import numpy as np
folder_path = "/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/05_Data/IDF_road_network_graph"
print("setup graph.")
g = Graph(directed=False)
v_coords = g.new_vertex_property("vector<float>")
g.vertex_properties["v_coords"] = v_coords
e_dist = g.new_edge_property("int")
g.edge_properties["e_dist"] = e_dist
eW = g.new_edge_property("float")
vW = g.new_vertex_property("float")
print("done.")
# Typical Manhattan block size = 80m x 275m
print("populating graph - 1.")
i = 0
iedges = {}
while i < 1120:
    print(i)
    jedges = []
    j = 0
    while j < 2800:
        v = g.add_vertex()
        v_coords[v] = (j, i)
        jedges.append(v)
        if j not in iedges:
            iedges[j] = []
        iedges[j].append(v)
        k = 0
        if ((i > 320 + 160 and i < 720+160) and (j == 1120 or j == 1260))\
            or ((i > 160 and i < 480+80) and (j == 560-560 or j == 700-560)) \
            or ((i > 80 + 240 and i < 400 + 320) and (j == 1680 - 280 or j == 1820 - 280)):
            j += 140
        else:
            j += 280
    if i != 0:
        while k < len(jedges) - 1:
            e = g.add_edge(jedges[k], jedges[k + 1])
            k += 1
    i += 80
for val in iedges.values():
    n = 0
    while n < len(val) - 1:
        g.add_edge(val[n], val[n + 1])
        n += 1

print("populating graph - 2.")
i = 1700
iedges = {}
while i < 1700+1120:
    print(i)
    jedges = []
    j=0
    while j < 2800:
        v = g.add_vertex()
        v_coords[v] = (j, i)
        jedges.append(v)
        if j not in iedges:
            iedges[j] = []
        iedges[j].append(v)
        j += 280
    k = 0
    if i != 1700+1040:
        while k < len(jedges)-1:
            g.add_edge(jedges[k], jedges[k+1])
            k += 1
    i += 80
print("adding val for edges")
for val in iedges.values():
    print(val)
    n=0
    while n < len(val) - 1:
        g.add_edge(val[n], val[n+1])
        n += 1

g.add_edge(143, 153)
g.add_edge(146, 156)
g.add_edge(149, 159)


for e in g.edges():
    print(e)
    e_dist[e] = int(np.sum(v_coords[e.target()].a - v_coords[e.source()].a))

print("plotting graph")
graph_draw(g,
           output_size=(3000, 3000), pos=v_coords,
           vertex_color=[0.75, 0.75, 0.75, 1], vertex_pen_width=0.4,
           vertex_size=5, vertex_text=g.vertex_index, edge_text=e_dist, output=folder_path+"CC_.png")
print("setup complete, saving.")
g.save(folder_path + "PlazaV3.xml.gz")

