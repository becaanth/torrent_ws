from rpe_viz import Posegraph, Edge, Vertex
import pdb
import numpy as np
import matplotlib.pyplot as plt
pg = Posegraph.load('/home/asrl/ASRL/vtr3/torrent_ws/deconstructed/0/woody_convoy/woody_convoy.pkl')

# inspect raw xi values
for e in pg.edge_list:
    print(e.from_id, e.to_id, e.xi)

# try out SE3 chaining without running the full monitor
pdb.set_trace()