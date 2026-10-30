"""
ttr_geo.py -- put VTR posegraph positions on a satellite basemap.

VTR positions (export_vertex_positions.py) are metres in the ROOT vertex's frame. With the root's
geographic pose (latitude, longitude, heading) they are placed on the map the way VTR's graph_map_server
projects the graph for its GUI (to my understanding of that code):

    [E, N] = UTM(lat0, lon0) + scale * R(theta) @ [x, y]        (UTM zone of the root)

theta = heading of the root's x axis, radians counter-clockwise from east (VTR's MapInfo.theta /
graph_projection.origin_theta). If the overlay comes out rotated or mirrored, tune with
--rotate_deg / --flip_y / --offset_m and check with --only map_check.

Coordinates are returned in Web Mercator (EPSG:3857), the projection of the satellite tiles.
"""
import math

import numpy as np
from pyproj import Transformer


def utm_epsg(lat, lon):
    zone = int((lon + 180.0) // 6.0) + 1
    return (32600 if lat >= 0 else 32700) + zone


class GeoFrame:
    def __init__(self, lat, lon, theta, scale=1.0, rotate_deg=0.0, flip_y=False, offset_m=(0.0, 0.0)):
        self.lat, self.lon = lat, lon
        self.theta = theta + math.radians(rotate_deg)
        self.scale = scale
        self.flip_y = flip_y
        self.offset = np.asarray(offset_m, dtype=float)
        epsg = utm_epsg(lat, lon)
        self._ll2utm = Transformer.from_crs("EPSG:4326", f"EPSG:{epsg}", always_xy=True)
        self._utm2merc = Transformer.from_crs(f"EPSG:{epsg}", "EPSG:3857", always_xy=True)
        self.e0, self.n0 = self._ll2utm.transform(lon, lat)
        c, s = math.cos(self.theta), math.sin(self.theta)
        self.R = np.array([[c, -s], [s, c]])

    def to_mercator(self, xy):
        """(N, 2) metres in the root frame -> (N, 2) Web Mercator metres."""
        xy = np.atleast_2d(np.asarray(xy, dtype=float)).copy()
        if self.flip_y:
            xy[:, 1] = -xy[:, 1]
        en = self.scale * xy @ self.R.T + self.offset + [self.e0, self.n0]
        mx, my = self._utm2merc.transform(en[:, 0], en[:, 1])
        return np.column_stack([mx, my])

    def mercator_per_metre(self):
        """Web Mercator units per true metre at the root (for a scale bar)."""
        return 1.0 / math.cos(math.radians(self.lat))


def to_mercator_dict(pos, geo):
    keys = list(pos)
    xy = geo.to_mercator(np.array([pos[k] for k in keys]))
    return {k: (float(p[0]), float(p[1])) for k, p in zip(keys, xy)}

def add_satellite(ax, source="Esri.WorldImagery", zoom="auto", cache_dir=None, attribution=True):
    import contextily as ctx
    if cache_dir:
        ctx.set_cache_dir(cache_dir)
    provider = ctx.providers
    for part in source.split("."):
        provider = provider[part]
    try:
        ctx.add_basemap(ax, crs="EPSG:3857", source=provider, zoom=zoom,
                        attribution=provider.get("attribution", "") if attribution else False,
                        attribution_size=6, zorder=0)
        if ax.images:
            im = ax.images[-1]
            from matplotlib.patches import PathPatch
            from matplotlib.path import Path
            import numpy as np
            x0, x1 = ax.get_xlim()
            y0, y1 = ax.get_ylim()
            verts = [(x0,y0),(x1,y0),(x1,y1),(x0,y1),(x0,y0)]
            clip = PathPatch(Path(verts), transform=ax.transData, visible=False)
            ax.add_patch(clip)
            im.set_clip_path(clip)
            print(f"  [debug] clip_path: {im.get_clip_path()}")
            print(f"  [debug] can_composite: {im.can_composite()}")
    except Exception as e:
        print(f"  basemap unavailable ({type(e).__name__}: {str(e)[:100]}); continuing without imagery")
        return False


def add_scalebar(ax, geo, length_m=None):
    """Scale bar in true metres (Web Mercator stretches distances by 1/cos(latitude))."""
    x0, x1 = ax.get_xlim()
    y0, y1 = ax.get_ylim()
    k = geo.mercator_per_metre()
    width_m = (x1 - x0) / k
    if length_m is None:
        raw = width_m / 5
        mag = 10 ** math.floor(math.log10(raw))
        length_m = max(m * mag for m in (1, 2, 5) if m * mag <= raw)
    bx, by = x0 + 0.04 * (x1 - x0), y0 + 0.05 * (y1 - y0)
    ax.plot([bx, bx + length_m * k], [by, by], color="white", lw=4, solid_capstyle="butt", zorder=6)
    ax.plot([bx, bx + length_m * k], [by, by], color="black", lw=1.5, solid_capstyle="butt", zorder=7)
    ax.text(bx + length_m * k / 2, by + 0.015 * (y1 - y0), f"{length_m:g} m", ha="center", va="bottom",
            fontsize=8, color="white", zorder=7,
            path_effects=[__import__("matplotlib.patheffects", fromlist=["x"]).withStroke(linewidth=2, foreground="black")])