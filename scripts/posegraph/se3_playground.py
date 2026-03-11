from rpe_viz import Posegraph, Edge, Vertex
import pdb
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Ellipse
import pylgmath

pg_name = 'test_indoors'
pg = Posegraph.load(f'/home/asrl/ASRL/vtr3/torrent_ws/deconstructed/0/{pg_name}/{pg_name}.pkl')

def compose_with_cov(
    T_prev: pylgmath.TransformationWithCovariance,
    T_edge: pylgmath.TransformationWithCovariance,
) -> pylgmath.TransformationWithCovariance:
    """
    Compose two TransformationWithCovariance and propagate covariance.
    cov_k = Ad(T_edge^-1) @ cov_{k-1} @ Ad(T_edge^-1).T + cov_edge
    """
    T_composed = pylgmath.TransformationWithCovariance(
        T_ba=(T_prev @ T_edge).matrix()
    )
    Ad_inv = T_edge.inverse().adjoint()
    cov_composed = Ad_inv @ T_prev.cov() @ Ad_inv.T + T_edge.cov()
    T_composed.set_covariance(cov_composed)
    # T_composed.set_covariance(T_edge.cov()) #if we want to see the raw value
    return T_composed

# inspect raw xi values
T = []
T_odom = [pylgmath.TransformationWithCovariance(init_cov_to_zero=True)]
for e in pg.edge_list:
    T_edge = pylgmath.TransformationWithCovariance(
        xi_ab=e.xi.reshape(6,1), covariance=e.cov
    )
    T_odom.append(compose_with_cov(T_odom[-1], T_edge))

# try out SE3 chaining without running the full monitor
xs = [float(T.r_ab_inb()[0]) for T in T_odom]
ys = [float(T.r_ab_inb()[1]) for T in T_odom]
covs = [T.cov() for T in T_odom[1:]]  # skip root, cov is zero

# pdb.set_trace()
# plt.plot(xs, ys)
# plt.show()


fig, ax = plt.subplots(figsize=(10, 8))
ax.plot(xs, ys, '-', color='steelblue', linewidth=0.8)
ax.scatter(xs, ys, s=8, color='steelblue', zorder=3)
ax.scatter(xs[0], ys[0], s=80, marker='*', color='gold', zorder=5, label='start')

EVERY_N = 10  # tune based on graph density
for i, cov in enumerate(covs[::EVERY_N]):
    xi = xs[i * EVERY_N + 1]  # +1 because covs skips root
    yi = ys[i * EVERY_N + 1]

    # x-y translation block — indices [0,1] assuming [rho; phi] ordering
    # cov_xy = cov[np.ix_([0, 1], [0, 1])]
    cov_xy = cov[0:2,0:2]

    eigvals, eigvecs = np.linalg.eigh(cov_xy)
    eigvals = np.maximum(eigvals, 0)  # numerical safety for near-zero values

    width  = 3 * 2 * np.sqrt(eigvals[0])  # 3-sigma
    height = 3 * 2 * np.sqrt(eigvals[1])
    angle  = np.degrees(np.arctan2(eigvecs[1, 0], eigvecs[0, 0]))

    ax.add_patch(Ellipse(
        xy=(xi, yi), width=width, height=height, angle=angle,
        edgecolor='tomato', facecolor='tomato', alpha=0.1, linewidth=0.6,
    ))

ax.set_aspect('equal')
ax.grid(True, linewidth=0.4, alpha=0.1)
ax.legend()
ax.set_title('Odometry with 3σ uncertainty ellipses')
plt.tight_layout()
plt.show()