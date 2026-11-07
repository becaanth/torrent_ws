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

class ViewRotation:
    """
    Display-only rotation of the map: Web Mercator points are turned `deg` degrees CCW about `center`,
    so a route network lying along a diagonal can be laid out wide. Distances are unchanged, so the
    scale bar still holds; north is no longer up (see add_north_arrow).
    """

    def __init__(self, deg, center):
        self.deg = float(deg)
        self.center = np.asarray(center, dtype=float)
        c, s = math.cos(math.radians(self.deg)), math.sin(math.radians(self.deg))
        self.R = np.array([[c, -s], [s, c]])

    def apply(self, xy):
        """(N, 2) Web Mercator -> (N, 2) rotated view coordinates."""
        xy = np.atleast_2d(np.asarray(xy, dtype=float))
        return (xy - self.center) @ self.R.T + self.center

    def inverse(self, xy):
        """(N, 2) rotated view coordinates -> (N, 2) Web Mercator."""
        xy = np.atleast_2d(np.asarray(xy, dtype=float))
        return (xy - self.center) @ self.R + self.center

    def mpl_transform(self, ax):
        """Matplotlib transform that draws Web Mercator data (e.g. an image) into the rotated view."""
        from matplotlib.transforms import Affine2D
        return Affine2D().rotate_deg_around(self.center[0], self.center[1], self.deg) + ax.transData


def add_satellite(ax, source="Esri.WorldImagery", zoom="auto", cache_dir=None, attribution=True, view=None):
    """Satellite tiles under the current axes limits. With a ViewRotation the limits are in the rotated
    view: tiles are fetched for the Web Mercator box around the whole view and drawn rotated."""
    import contextily as ctx
    if cache_dir:
        ctx.set_cache_dir(cache_dir)
    provider = ctx.providers
    for part in source.split("."):
        provider = provider[part]
    attr = provider.get("attribution", "") if attribution else False
    if not attribution:
        print(f"  imagery attribution left off the figure; credit it in the caption: "
              f"\"{provider.get('attribution', source)}\"")
    try:
        if view is None:
            ctx.add_basemap(ax, crs="EPSG:3857", source=provider, zoom=zoom,
                            attribution=attr, attribution_size=6, zorder=0)
            return True
        x0, x1 = ax.get_xlim()
        y0, y1 = ax.get_ylim()
        corners = view.inverse([[x0, y0], [x1, y0], [x1, y1], [x0, y1]])
        (w, s), (e, n) = corners.min(axis=0), corners.max(axis=0)
        img, ext = ctx.bounds2img(w, s, e, n, zoom=zoom, source=provider, ll=False)
        ax.imshow(img, extent=ext, origin="upper", interpolation="bilinear", zorder=0,
                  transform=view.mpl_transform(ax))      # clipped to the axes box like any artist
        ax.set_xlim(x0, x1)                              # imshow autoscales; keep the view
        ax.set_ylim(y0, y1)
        if attr:
            ctx.add_attribution(ax, attr, font_size=6)
        return True
    except Exception as e:
        print(f"  basemap unavailable ({type(e).__name__}: {str(e)[:100]}); continuing without imagery")
        return False


def add_north_arrow(ax, view, center=(0.07, 0.80), length_pt=26):
    """Arrow and 'N' pointing to true north for a rotated view, centred at `center` (axes fractions).
    The arrow is laid out in points (isotropic), so its direction holds whatever the axes' final shape."""
    import matplotlib.patheffects as pe
    from matplotlib.transforms import offset_copy
    a = math.radians(90.0 + view.deg)                    # north in the view, CCW from the view's x axis
    ux, uy = math.cos(a), math.sin(a)
    h = length_pt / 2

    def at(d):                                           # `center` moved d points towards north
        return offset_copy(ax.transAxes, fig=ax.figure, x=d * ux, y=d * uy, units="points")

    stroke = [pe.withStroke(linewidth=3, foreground="black")]
    ax.annotate("", xy=center, xycoords=at(h), xytext=center, textcoords=at(-h), zorder=8,
                annotation_clip=False,
                arrowprops=dict(arrowstyle="-|>,head_width=0.35,head_length=0.7", color="white", lw=2,
                                shrinkA=0, shrinkB=0, path_effects=stroke))
    ax.text(*center, "N", transform=at(h + 9), ha="center", va="center", fontsize=10, fontweight="bold",
            color="white", zorder=8, path_effects=stroke)


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