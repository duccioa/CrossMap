from graph_tool.all import *
import datetime
import numpy as np
from math import log

print(datetime.datetime.now())
startTime = datetime.datetime.now()
# Load and setup the graph:
folder_path = "/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/05_Data/IDF_road_network_graph"
print("loading graph")
g = load_graph(folder_path + "PlazaV3.xml.gz")
remove_parallel_edges(g)
remove_self_loops(g)
# Loaded property maps
v_coords = g.vertex_properties["v_coords"]
e_dist = g.edge_properties["e_dist"]
# New property maps
v_calc = g.new_vertex_property("float")
e_calc = g.new_edge_property("float")

g.vertex_properties["v_calc"] = v_calc
g.edge_properties["e_calc"] = e_calc
# set global parameters
dist = 600
print("total vertices: " + str(g.num_vertices()))
count = 0
batchTime = datetime.datetime.now()
# GLOBAL CLOSENESS
vW = closeness(g, weight=e_dist)
for v in g.vertices():
    v_calc[v] = vW[v]
# GLOBAL BETWEENNESS
vW, eW = betweenness(g, weight=e_dist)
for v in g.vertices():
    v_calc[v] = vW[v]

for v in g.vertices():
    count += 1

if (count % 1000 == 0):
    print("processed total of " + str(count) + ". Batch time: " + str(datetime.datetime.now() - batchTime))
    batchTime = datetime.datetime.now()
# reset edge filter
g.set_vertex_filter(None)
v_localDist = shortest_distance(g, source=v, weights=e_dist, max_dist=dist)
# Mask nodes outside distance threshold
v_mask = g.new_vertex_property("bool")
v_mask.a = v_localDist.a <= dist  # set the array values to 1 (boolean property map) where distance parameter is met
g.set_vertex_filter(v_mask)  # activate mask based on currently selected vertices
# vRes = nodeOutProb(g, v) # nodeNumba(g, v)
# v_calc[v] = vRes

#### LOCAL BETWEENNESS
vB, eB = betweenness(g, weight=e_dist, norm=False)
### ASSIGN LOCAL BETWEENNESS
v_calc[v] = vB[v]
#### LOCAL CLOSENESS
vB = closeness(g, weight=e_dist, norm=False)
v_calc[v] = -vB[v]
### NODE OUTWARDS INFORMATION ENTROPY
# Calculates probability of all possible routes from node within cutoff distance
visitedNodes = set()
claimedEdges = set()
branches = {}
probabilities = []
# add origin
branches[v] = 1  # add starting node with starting probability
visitedNodes.add(v)  # add starting node to visited set
# start iterating through branches...adding and removing as you go
while len(branches) > 0:
    for key, val in branches.items():
        print("key: ", key, "val: ", val)  # DEBUG
        newVal = 0.0
        newEdges = []
        for e in key.all_edges():
            print("\te: ", e)  # DEBUG
            if e not in claimedEdges:
                newVal += 1
                claimedEdges.add(e)
                newEdges.append(e)
        if newVal == 0:
            probabilities.append(val)
        else:
            for n in key.all_neighbours():
                print("\t\tn: ", n)  # DEBUG
                if n not in visitedNodes:
                    visitedNodes.add(n)
                    branches[n] = newVal * val
                # elif g.edge(key, n) in newEdges:
                # probabilities.append(newVal * val)
        del branches[key]
r = 0
if len(probabilities) == 1:
    v_calc[v] = 0
else:
    for k in probabilities:
        r += (1 / k) * log(1 / k, 2)
    v_calc[v] = -r

### NODE ENTROPY
# crude method for summing relevant degrees and approximating log2 information of n degrees.
# More robust version is per Raychaudhury et. al. which preserves log2 complexity of degree type choices.
# (See next.)
# Note that degrees are counted as -2, which nuetralises cul-de-sacs and random waypoint nodes in ITN data
# and reduces 3-out nodes to "1" bit route (actually 1.6) choice and 4-out nodes to 2 bit choice. It then calculates the maximal
# log(n) entropy using all route choices as equal probabilities.
x = [vert.out_degree() - 2 for vert in g.vertices()]
v_calc[v] = float(sum(x))
# Raychaudhury et al
x = [vert.out_degree() for vert in g.vertices()]
s = float(sum(x))
if s != 0:
    d1 = [val for val in x if val == 1]
    d2 = [val for val in x if val == 2]
    d3 = [val for val in x if val == 3]
    d4 = [val for val in x if val == 4]
    d5 = [val for val in x if val == 5]
    d6 = [val for val in x if val == 6]
    v_calc[v] = -(len(d1) * 1 / s * log(1 / s, 2) \
                  + len(d2) * 2 / s * log(2 / s, 2) \
                  + len(d3) * 3 / s * log(3 / s, 2) \
                  + len(d4) * 4 / s * log(4 / s, 2) \
                  + len(d5) * 5 / s * log(5 / s, 2) \
                  + len(d6) * 6 / s * log(6 / s, 2))

### Vertices
x = vW.a[vW.a > 0]
xt = np.sum(x)
if xt > 0:
    v_calc[v] = -np.sum([(i / xt) * log(i / xt, 2) for i in x])
### Edges
y = eW.a[eW.a > 0]
yt = np.sum(y)
for e in v.out_edges():
    if yt > 0:
        e_calc[e] += -np.sum([(i / yt) * log(i / yt, 2) for i in y]) * 0.5
### MESHEDNESS | Alpha index e-n+1/2(n)-5
vCount = g.num_vertices()
eCount = g.num_edges()
v_calc[v] = (eCount - vCount + 1) / ((2 * float(vCount)) - 5)

### BETA INDEX e / n
vCount = g.num_vertices()
eCount = g.num_edges()
v_calc[v] = eCount / float(vCount)
### GAMMA INDEX e/3(v-2)
vCount = g.num_vertices()
eCount = g.num_edges()
if vCount > 2:
    v_calc[v] = eCount / (3 * (vCount - 2.0))

# reset edge filter
g.set_vertex_filter(None)
for e in g.edges():
    e_calc[e] = v_calc[e.source()] + v_calc[e.target()]

g.save(folder_path + "GBn_NodeOutwardsEntropy_" + str(dist) + ".xml.gz")

print("time: " + str(datetime.datetime.now() - startTime))
print("plotting")
graph_draw(g,
           output_size=(1000, 1000),
           pos=v_coords,
           vertex_color=[0.5, 0.5, 0.5, 1],
           vertex_fill_color=v_calc,
           vertex_size=prop_to_size(v_calc, mi=1, ma=12),
           vertex_pen_width=0.8,
           edge_color=(0.5, 0.5, 0.5, 1),
           edge_pen_width=prop_to_size(e_calc, mi=0.5, ma=2),
           output=folder_path + "_outName_" + str(dist) + ".png")
print("total time: " + str(datetime.datetime.now() - startTime))