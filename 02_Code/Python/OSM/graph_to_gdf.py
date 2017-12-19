
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
from shapely.geometry import LineString

from networkx.utils.misc import make_str


def graph_to_gdf(G, nodes=True, edges=True, node_geometry=True, fill_edge_geometry=True):
    """
    Convert a graph into node and/or edge GeoDataFrames

    Parameters
    ----------
    G : networkx multidigraph
    nodes : bool
        if True, convert graph nodes to a GeoDataFrame and return it
    edges : bool
        if True, convert graph edges to a GeoDataFrame and return it
    node_geometry : bool
        if True, create a geometry column from node x and y data
    fill_edge_geometry : bool
        if True, fill in missing edge geometry fields using origin and
        destination nodes

    Returns
    -------
    GeoDataFrame or tuple
        gdf_nodes or gdf_edges or both as a tuple
    """

    if not (nodes or edges):
        raise ValueError('You must request nodes or edges, or both.')

    to_return = []

    if nodes:

        nodes = {node: data for node, data in G.nodes(data=True)}
        gdf_nodes = gpd.GeoDataFrame(nodes).T
        if node_geometry:
            gdf_nodes['geometry'] = gdf_nodes.apply(lambda row: Point(row['x'], row['y']), axis=1)
        gdf_nodes.crs = G.graph['crs']
        gdf_nodes.gdf_name = '{}_nodes'.format(G.graph['name'])
        gdf_nodes['osmid'] = gdf_nodes['osmid'].astype(np.int64).map(make_str)

        to_return.append(gdf_nodes)

    if edges:

        # create a list to hold our edges, then loop through each edge in the
        # graph
        edge_num = 1
        edges = []
        for u, v, data in G.edges(data=True):
            #print(edge_num)
            edge_num += 1
            # for each edge, add key and all attributes in data dict to the
            # edge_details
            edge_details = {'u': u, 'v': v}
            for key, value in data.items():
                edge_details[key] = value

            # if edge doesn't already have a geometry attribute, create one now
            # if fill_edge_geometry==True
            if 'geometry' not in data:
                if fill_edge_geometry:
                    point_u = Point((G.nodes[u]['x'], G.nodes[u]['y']))
                    point_v = Point((G.nodes[v]['x'], G.nodes[v]['y']))
                    edge_details['geometry'] = LineString([point_u, point_v])
                else:
                    edge_details['geometry'] = np.nan

            edges.append(edge_details)

        # create a GeoDataFrame from the list of edges and set the CRS
        gdf_edges = gpd.GeoDataFrame(edges)
        gdf_edges.crs = G.graph['crs']
        #gdf_edges.gdf_name = '{}_edges'.format(G.graph['name'])

        to_return.append(gdf_edges)

    if len(to_return) > 1:
        return tuple(to_return)
    else:
        return to_return[0]
