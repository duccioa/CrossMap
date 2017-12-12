import networkx as nx
from matplotlib import pyplot as plt
import seaborn as sns
import pandas as pd
try:
    from osgeo import ogr
except ImportError:
    raise ImportError("read_shp requires OGR: http://www.gdal.org/")

path = "/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/05_Data/OpenStreetMap/IDF_roads/osm_idf_roads.shp"


def edges_from_line(geom, attrs, simplify=True, geom_attrs=True, export_geom='Wkt'):
    """
    Generate edges for each line in geom
    Written as a helper for read_shp

    Parameters
    ----------

    geom:  ogr line geometry
        To be converted into an edge or edges

    attrs:  dict
        Attributes to be associated with all geoms

    simplify:  bool
        If True, simplify the line as in read_shp

    geom_attrs:  bool
        If True, add geom attributes to edge as in read_shp


    Returns
    -------
     edges:  generator of edges
        each edge is a tuple of form
        (node1_coord, node2_coord, attribute_dict)
        suitable for expanding into a networkx Graph add_edge call
    """
    try:
        from osgeo import ogr
    except ImportError:
        raise ImportError("edges_from_line requires OGR: http://www.gdal.org/")

    if geom.GetGeometryType() == ogr.wkbLineString:
        if simplify:
            edge_attrs = attrs.copy()
            last = geom.GetPointCount() - 1
            if geom_attrs:
                if export_geom == 'Wkb':
                    edge_attrs["Wkb"] = geom.ExportToWkb()
                elif export_geom == 'Wkt':
                    edge_attrs["Wkt"] = geom.ExportToWkt()
                elif export_geom == 'Json':
                    edge_attrs["Json"] = geom.ExportToJson()
                else:
                    pass
            yield (geom.GetPoint_2D(0), geom.GetPoint_2D(last), edge_attrs)
        else:
            for i in range(0, geom.GetPointCount() - 1):
                pt1 = geom.GetPoint_2D(i)
                pt2 = geom.GetPoint_2D(i + 1)
                edge_attrs = attrs.copy()
                if geom_attrs:
                    segment = ogr.Geometry(ogr.wkbLineString)
                    segment.AddPoint_2D(pt1[0], pt1[1])
                    segment.AddPoint_2D(pt2[0], pt2[1])
                    if export_geom == 'Wkb':
                        edge_attrs["Wkb"] = segment.ExportToWkb()
                    elif export_geom == 'Wkt':
                        edge_attrs["Wkt"] = segment.ExportToWkt()
                    elif export_geom == 'Json':
                        edge_attrs["Json"] = segment.ExportToJson()
                    else:
                        pass
                    del segment
                yield (pt1, pt2, edge_attrs)

    elif geom.GetGeometryType() == ogr.wkbMultiLineString:
        for i in range(geom.GetGeometryCount()):
            geom_i = geom.GetGeometryRef(i)
            for edge in edges_from_line(geom_i, attrs, simplify, geom_attrs):
                yield edge


net = nx.DiGraph()
shp = ogr.Open(path)
for lyr in shp:
    fields = [x.GetName() for x in lyr.schema]
    for i in range(0, len(lyr)):
        print(f"Feature: {i}/{len(lyr)}")
        f = lyr[i]
        flddata = [f.GetField(f.GetFieldIndex(x)) for x in fields]
        g = f.geometry()
        attributes = dict(zip(fields, flddata))
        attributes["ShpName"] = lyr.GetName()
        # Note:  Using layer level geometry type
        if g.GetGeometryType() == ogr.wkbPoint:
            net.add_node((g.GetPoint_2D(0)), attributes)
        elif g.GetGeometryType() in (ogr.wkbLineString,
                                     ogr.wkbMultiLineString):
            for edge in edges_from_line(g, attributes, True,
                                        True):
                e1, e2, attr = edge
                net.add_edge(e1, e2)
                net[e1][e2].update(attr)
        else:
            raise ImportError("GeometryType {} not supported".
                              format(g.GetGeometryType()))

nx.write_graphml(net, "/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/05_Data/OpenStreetMap/roads_graph_IDF.graphml")

print(net.number_of_edges())
print(net.number_of_nodes())
print(net.number_of_selfloops())

remove = [degree[0] for degree in list(net.degree()) if degree[1] < 2]

d = []
for l in list(net.degree):
    d.append(l[1])
dd = pd.Series(d)
print(dd.unique())

net.remove_nodes_from(remove)

sns.distplot(pd.Series(d))
plt.show(block=True)
