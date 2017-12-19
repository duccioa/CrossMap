import itertools
from multiprocessing import Pool
import networkx as nx
"""
Example of parallel implementation of betweenness centrality using the
multiprocessing module from Python Standard Library.

The function betweenness centrality accepts a bunch of nodes and computes
the contribution of those nodes to the betweenness centrality of the whole
network. Here we divide the network in chunks of nodes and we compute their
contribution to the betweenness centrality of the whole network.
"""
def chunks(l, n):
    """Divide a list of edges `l` in `n` chunks"""
    l_c = iter(l)
    while 1:
        x = tuple(itertools.islice(l_c, n))
        if not x:
            return
        yield x


def _betmap100(G_normalized_weight_sources_tuple):
    """Pool for multiprocess only accepts functions with one argument.
    This function uses a tuple as its only argument. We use a named tuple for
    python 3 compatibility, and then unpack it when we send it to
    `betweenness_centrality_source`
    """
    return nx.edge_betweenness_centrality(*G_normalized_weight_sources_tuple, k=100, weight='length', normalized=True)

def _betmap1000(G_normalized_weight_sources_tuple):
    """Pool for multiprocess only accepts functions with one argument.
    This function uses a tuple as its only argument. We use a named tuple for
    python 3 compatibility, and then unpack it when we send it to
    `betweenness_centrality_source`
    """
    return nx.edge_betweenness_centrality(*G_normalized_weight_sources_tuple, k=1000, weight='length', normalized=True)


def edge_betweenness_centrality_parallel(G, processes=None, k=100):
    """Parallel betweenness centrality  function"""
    p = Pool(processes=processes)
    edge_divisor = len(p._pool)*4
    edge_chunks = list(chunks(G.edges(), int(G.order()/edge_divisor)))
    num_chunks = len(edge_chunks)
    if k == 1000:
        betmap_fun = _betmap1000
    else:
        betmap_fun = _betmap100
    bt_sc = p.map(betmap_fun,
                  zip([G]*num_chunks,
                      [True]*num_chunks,
                      [None]*num_chunks,
                      edge_chunks))

    # Reduce the partial solutions
    bt_c = bt_sc[0]
    for bt in bt_sc[1:]:
        for n in bt:
            bt_c[n] += bt[n]
    return bt_c



