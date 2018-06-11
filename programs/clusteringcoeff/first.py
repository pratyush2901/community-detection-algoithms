import networkx as nx
import time

G=nx.read_edgelist("input.txt")
start_time = time.time()
nx.clustering(G)
print("--- %s seconds ---" % (time.time() - start_time))