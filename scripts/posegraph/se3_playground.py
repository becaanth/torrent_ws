from rpe_viz import Posegraph, Edge, Vertex
import pdb
import numpy as np
import matplotlib.pyplot as plt
import pylgmath

pg = Posegraph.load('/home/asrl/ASRL/vtr3/torrent_ws/deconstructed/0/woody_convoy/woody_convoy.pkl')

# inspect raw xi values
T = []
for e in pg.edge_list:
    print(e.from_id, e.to_id, e.xi)

    t = pylgmath.Transformation(xi_ab=e.xi.reshape(6,1))
    T.append(t.matrix())

# try out SE3 chaining without running the full monitor

T_odom = []
T0 = np.eye(4)
T_odom.append(T0)
for i, t in enumerate(T):
    if i>0:
        T_odom.append(T_odom[i-1] @ t)

np_odom = np.array(T_odom)

plt.plot(np_odom[:,0,3], np_odom[:,1,3])
plt.show()