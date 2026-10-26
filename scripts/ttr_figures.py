#!/usr/bin/env python3
"""
ttr_figures.py -- figures and animations for a Teach, Torrent, Repeat run.

    python3 ttr_figures.py <logs dir or files ...> -o figures/ [--positions vertex_positions.csv]
           [--only fig1,fig2,fig3,anim3,anim4] [--bin 10] [--speed 20] [--fps 10]

Reads every robot's orchestrator logs (rotated parts included) through ttr_events.load().

Figures (PNG + PDF):
  fig1_maps        maps over time: submaps created by the teacher, published in its snapshots,
                   and downloaded / ingested by each peer
  fig2_bandwidth   network-wide payload received over time, by type: pieces, metadata, gossip
                   (rate per --bin seconds, and cumulative)
  fig3_upload      upload contribution per robot: total bytes sent, split by recipient,
                   and cumulative upload over time
Animations (MP4, or GIF with --gif):
  anim3_positions  robots on the teach graph over time
  anim4_transfers  the same, with an arrow from sender to receiver for every active transfer,
                   width proportional to its rate
Tables (CSV): flows_1s.csv (src, dst, bytes per second), upload_matrix.csv (sent vs received, per pair)

Positions: pass --positions (vid,x,y from export_vertex_positions.py) for the real map.
Without it the animations use a strip layout: distance along the teach sequences on x
(minor id x 0.3 m, sequences laid end to end), one lane per robot.
"""
import argparse
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import animation
from matplotlib.patches import FancyArrowPatch
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import ttr_events as te
from animate_robots import (ROBOT_NAMES, ROBOT_COLORS, MINOR_SPACING, read_positions,
                            active_spans, merge, track, position_at, fmt_t)

MB = 1e6
LINE_COLORS = dict(enumerate(ROBOT_COLORS))
LINE_COLORS[2] = "#d9a900"                      # col_mustard: darker yellow so lines read on white
TYPE_COLORS = {"pieces": "#1f4e79", "metadata": "#c55a11", "gossip": "#7f7f7f"}


def name(r):
    try:
        r = int(r)
    except (TypeError, ValueError):
        return "unknown"
    return f"{ROBOT_NAMES.get(r, 'robot')} ({r})"


def color(r):
    try:
        return LINE_COLORS[int(r) % len(ROBOT_COLORS)]
    except (TypeError, ValueError):
        return "#999999"


# ======================================================================= data prep
def fill_peer_ids(df, ev):
    """xfer/peer rows logged before the handshake finished have no peer id: recover it from the
    address, using other rows where the same observing robot saw that address with an id."""
    known = []
    for k in ("xfer", "peer"):
        if k in ev:
            known.append(ev[k][["robot", "addr", "peer"]].dropna())
    if not known:
        return df
    amap = pd.concat(known).drop_duplicates(["robot", "addr"]).set_index(["robot", "addr"])["peer"]
    df = df.copy()
    miss = df["peer"].isna()
    df.loc[miss, "peer"] = [amap.get((r, a), pd.NA) for r, a in zip(df.loc[miss, "robot"], df.loc[miss, "addr"])]
    return df


STARTED = set()   # robots whose logs include the orchestrator start (set in main)


def robots_with_start(paths):
    """Robots whose logs include the first part ("Logging initialized"). For the others the logs
    begin mid-run, so the first value of each cumulative counter is history, not new data."""
    found, cache = set(), {}
    for path in te._files(paths):
        with open(path, errors="replace") as f:
            for _, line in zip(range(50), f):
                if "Logging initialized" in line:
                    found.add(te._robot_of(path, cache))
                    break
    return found


def xfer_deltas(ev, direction):
    """Bytes moved per xfer sample (cumulative counters; a drop means the process restarted)."""
    if "xfer" not in ev:
        return pd.DataFrame(columns=["t", "robot", "peer", "addr", "bytes", "delta"])
    x = fill_peer_ids(ev["xfer"], ev)
    x = x[x["dir"] == direction].sort_values("t").copy()
    prev = x.groupby(["robot", "peer", "addr"], dropna=False)["bytes"].shift()
    d = x["bytes"] - prev
    first = prev.isna()
    x["delta"] = d.where(d >= 0, x["bytes"])
    # first sample of a series: all of it is new only if the log starts with the process
    x.loc[first, "delta"] = np.where(x.loc[first, "robot"].isin(STARTED), x.loc[first, "bytes"], 0)
    return x


def log_windows(ev):
    """robot -> (first, last) time covered by that robot's logs."""
    allt = pd.concat([d[["robot", "t"]] for d in ev.values()])
    g = allt.groupby("robot")["t"]
    return {int(r): (lo, hi) for r, lo, hi in zip(g.min().index, g.min(), g.max())}


def flows(ev, period=1.0):
    """
    (t, src, dst, bytes): data moved between robots.
    Uses the receiver's 'down' counters while the receiver's logs cover that time, and the
    sender's 'up' counters otherwise (receiver log missing, or starting/ending earlier).
    """
    win = log_windows(ev)
    down = xfer_deltas(ev, "down").rename(columns={"peer": "src", "robot": "dst"})
    up = xfer_deltas(ev, "up").rename(columns={"robot": "src", "peer": "dst"})

    receivers = set(int(r) for r in down["dst"].dropna().unique())   # robots that logged xfer 'down'

    def covered(dst, t):
        if pd.isna(dst) or int(dst) not in receivers:
            return False
        lo, hi = win[int(dst)]
        return lo <= t <= hi
    if len(up):
        keep = [not covered(d, t) for d, t in zip(up["dst"], up["t"])]
        up = up[keep]
    f = pd.concat([down[["t", "src", "dst", "delta"]], up[["t", "src", "dst", "delta"]]])
    f = f.dropna(subset=["src", "dst"])
    f["t"] = (f["t"] // period) * period
    out = f.groupby(["t", "src", "dst"])["delta"].sum().reset_index().rename(columns={"delta": "bytes"})
    out[["src", "dst"]] = out[["src", "dst"]].astype(int)
    return out


def upload_matrix(ev):
    up = xfer_deltas(ev, "up").groupby(["robot", "peer"])["delta"].sum()
    down = xfer_deltas(ev, "down").groupby(["peer", "robot"])["delta"].sum()
    pairs = sorted(set(up.index) | set(down.index), key=lambda p: (int(p[0]), int(p[1])))
    rows = [{"src": int(s), "dst": int(d),
             "sent_MB": round(up.get((s, d), np.nan) / MB, 3),
             "received_MB": round(down.get((s, d), np.nan) / MB, 3)} for s, d in pairs]
    return pd.DataFrame(rows)


def save(fig, out, stem, fmts):
    for f in fmts:
        fig.savefig(os.path.join(out, f"{stem}.{f}"), dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {stem}.{{{','.join(fmts)}}}")


def minutes(t, t0):
    return (np.asarray(t, dtype=float) - t0) / 60.0


# ======================================================================= figure 1
def fig1_maps(ev, t0, out, fmts, piece_size):
    fig, ax = plt.subplots(figsize=(10, 4.5))
    notes = []

    if "chunk" in ev and len(ev["chunk"]):
        c = ev["chunk"].sort_values("t")
        ax.step(minutes(c.t, t0), np.arange(1, len(c) + 1), where="post", color="black", lw=2,
                label="created by teacher")
    else:
        notes.append("no chunk events in these logs")

    if "snapshot" in ev and len(ev["snapshot"]):
        s = ev["snapshot"].sort_values("t")
        n = np.ceil(s["bytes"] / piece_size)
        ax.step(minutes(s.t, t0), n, where="post", color="black", lw=1.2, ls=":",
                label=f"published in snapshots (torrent size / {piece_size / 2**20:g} MiB)")

    for r, d in (ev["piece"].groupby("robot") if "piece" in ev else []):
        d = d.sort_values("t")
        ax.step(minutes(d.t, t0), np.arange(1, len(d) + 1), where="post", color=color(r), lw=1.8,
                label=f"downloaded by {name(r)}")
    for r, d in (ev["ingest"].groupby("robot") if "ingest" in ev else []):
        d = d.sort_values("t")
        ax.step(minutes(d.t, t0), np.arange(1, len(d) + 1), where="post", color=color(r), lw=1.2,
                ls="--", label=f"ingested by {name(r)}")

    ax.set_xlabel("time since start of run (min)")
    ax.set_ylabel("submaps (count)")
    ax.set_title("Submaps created by the teacher vs downloaded by each peer")
    ax.grid(alpha=0.3)
    ax.legend(fontsize=8, loc="upper left", bbox_to_anchor=(1.01, 1.0), borderaxespad=0)
    if notes:
        ax.text(0.99, 0.02, "; ".join(notes), transform=ax.transAxes, ha="right", fontsize=8, color="#a00")
    save(fig, out, "fig1_maps", fmts)


# ======================================================================= figure 2
def fig2_bandwidth(ev, t0, t1, out, fmts, bin_s):
    series = {}
    pf = flows(ev, 1.0)                                      # receivers' counts, senders' where missing
    series["pieces"] = (pf["t"].to_numpy() + 0.5, pf["bytes"].to_numpy())
    if "metadata" in ev:
        series["metadata"] = (ev["metadata"]["t"].to_numpy(), ev["metadata"]["bytes"].to_numpy())
    if "gossip_rx" in ev:
        series["gossip"] = (ev["gossip_rx"]["t"].to_numpy(), ev["gossip_rx"]["bytes"].to_numpy())

    edges = np.arange(t0, t1 + bin_s, bin_s)
    centers = minutes(edges[:-1] + bin_s / 2, t0)
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(8, 6.5), sharex=True)
    totals = {}
    for k, (t, b) in series.items():
        h, _ = np.histogram(t, bins=edges, weights=b.astype(float))
        totals[k] = b.sum()
        rate = h / bin_s / 1e3                               # kB/s
        a1.plot(centers, np.where(rate > 0, rate, np.nan), color=TYPE_COLORS[k], lw=1.4, marker=".", ms=3,
                label=f"{k}: {totals[k] / MB:,.2f} MB total")
        order = np.argsort(t)
        a2.plot(minutes(t[order], t0), np.cumsum(b[order]) / MB, color=TYPE_COLORS[k], lw=1.8, label=k)
    a1.set_yscale("log")
    a1.set_ylabel(f"received (kB/s, {bin_s:g} s bins)")
    a1.set_title("Network-wide payload received, by type")
    a1.legend(fontsize=8)
    a1.grid(alpha=0.3, which="both")
    a2.set_yscale("log")
    a2.set_ylabel("cumulative received (MB)")
    a2.set_xlabel("time since start of run (min)")
    a2.grid(alpha=0.3, which="both")
    a2.text(0.01, 0.02, "payload bytes; gossip excludes Zenoh/TCP framing, pieces exclude BitTorrent protocol "
            "messages (~0.1%)", transform=a2.transAxes, fontsize=7, color="#555")
    save(fig, out, "fig2_bandwidth", fmts)
    return totals


# ======================================================================= figure 3
def fig3_upload(ev, t0, out, fmts):
    up = xfer_deltas(ev, "up")
    down = xfer_deltas(ev, "down")
    # per uploader: from its own 'up' counters; for uploaders without logs, from receivers' 'down'
    sent = up.groupby(["robot", "peer"])["delta"].sum()
    logged_up = set(up["robot"].unique())
    seen = down.groupby(["peer", "robot"])["delta"].sum()
    extra = seen[[s not in logged_up for s, _ in seen.index]]
    contrib = pd.concat([sent, extra]).rename_axis(["src", "dst"])
    inferred = set(int(s) for s, _ in extra.index)

    robots = sorted({int(r) for r in contrib.index.get_level_values(0)} |
                    {int(r) for r in contrib.index.get_level_values(1)})
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 4.5), gridspec_kw={"width_ratios": [1, 1.4]})
    x = np.arange(len(robots))
    bottom = np.zeros(len(robots))
    for dst in robots:
        vals = np.array([contrib.get((s, dst), 0.0) for s in robots]) / MB
        if vals.sum() == 0:
            continue
        a1.bar(x, vals, bottom=bottom, color=color(dst), edgecolor="black", lw=0.5, label=f"to {name(dst)}")
        bottom += vals
    for i, r in enumerate(robots):
        a1.text(i, bottom[i], f"{bottom[i]:,.0f} MB" + ("*" if r in inferred else ""),
                ha="center", va="bottom", fontsize=8)
    a1.set_xticks(x, [name(r) for r in robots], rotation=20, ha="right", fontsize=8)
    a1.set_ylabel("uploaded (MB)")
    a1.set_ylim(0, max(bottom.max(), 1e-9) * 1.15)
    a1.set_title("Upload contribution, by recipient")
    a1.legend(fontsize=7, loc="upper right")
    if inferred:
        a1.text(0.0, -0.30, "* from the receivers' logs (uploader's log not provided)",
                transform=a1.transAxes, va="top", fontsize=7, color="#555")

    for r in robots:
        if r in logged_up:
            d = up[up["robot"] == r].sort_values("t")
            t, b = d["t"].to_numpy(), d["delta"].to_numpy()
        else:
            d = down[down["peer"] == r].sort_values("t")
            t, b = d["t"].to_numpy(), d["delta"].to_numpy()
        if len(t):
            a2.plot(minutes(t, t0), np.cumsum(b) / MB, color=color(r), lw=1.8,
                    ls="-" if r in logged_up else "--", label=name(r))
    a2.set_xlabel("time since start of run (min)")
    a2.set_ylabel("cumulative upload (MB)")
    a2.set_title("Upload over time")
    a2.grid(alpha=0.3)
    a2.legend(fontsize=8)
    fig.tight_layout()
    save(fig, out, "fig3_upload", fmts)


# ======================================================================= animations
def ev_times(ev, robot):
    """All event times logged by one robot, sorted (its log is 'alive' while these keep coming)."""
    return np.sort(np.concatenate([d.loc[d["robot"] == robot, "t"].to_numpy() for d in ev.values()]))


def make_geo(args):
    if not args.origin:
        return None
    from ttr_geo import GeoFrame
    lat, lon, theta = args.origin
    return GeoFrame(lat, lon, theta, scale=args.map_scale, rotate_deg=args.rotate_deg,
                    flip_y=args.flip_y, offset_m=args.offset_m)


def to_mercator_dict(pos, geo):
    from ttr_geo import to_mercator_dict as f
    return f(pos, geo)


def draw_map(ax, bg, geo, args, graph=True):
    """Teach graph (grey, or white over imagery) with limits, aspect, labels and optional satellite basemap."""
    span = max(np.ptp(bg[:, 0]), np.ptp(bg[:, 1]), 10.0)
    ax.set_xlim(bg[:, 0].min() - 0.06 * span, bg[:, 0].max() + 0.06 * span)
    ax.set_ylim(bg[:, 1].min() - 0.06 * span, bg[:, 1].max() + 0.06 * span)
    ax.set_aspect("equal")
    imagery = False
    if geo is not None and args.basemap != "none":
        from ttr_geo import add_satellite, add_scalebar
        imagery = add_satellite(ax, source=args.basemap, cache_dir=args.tile_cache)
        add_scalebar(ax, geo)
        ax.set_xticks([])
        ax.set_yticks([])
    else:
        ax.set_xlabel("x (m)" if geo is None else "Web Mercator x (m)")
        ax.set_ylabel("y (m)" if geo is None else "Web Mercator y (m)")
    if graph:
        ax.plot(bg[:, 0], bg[:, 1], ".", color="white" if imagery else "#cfcfcf",
                ms=2 if not imagery else 1.5, alpha=0.8 if imagery else 1.0, zorder=1)
    return imagery


def teacher_of(vid):
    """Robot that taught a vertex: the top 4 bits of its id."""
    return int(vid) >> 60


def read_stamps(path):
    """vid -> vertex timestamp in seconds, from export_vertex_positions.py's 'stamp' column (ns)."""
    import csv
    out = {}
    with open(path) as f:
        for r in csv.DictReader(f):
            try:
                st = float(r.get("stamp") or 0)
            except ValueError:
                continue
            if st > 0:
                out[int(r["vid"])] = st / 1e9 if st > 1e15 else st
    return out


def reveal_times(ev, vids, stamps, t_lo, t_hi):
    """
    When each vertex came into existence, for drawing the graph as it is taught.
      1. first time the robot that taught it logged it (its own vertex events during teach)
      2. else its timestamp from the positions CSV, if that is a plausible wall-clock time
      3. else -inf: taught before this recording (drawn faded from the first frame)
    Repeaters reaching a vertex never reveal it, so the map does not appear to grow where they drive.
    """
    first = {}
    if "vertex" in ev:
        v = ev["vertex"]
        for r, t, vid in zip(v["robot"], v["t"], v["vid"].astype("uint64")):
            vid = int(vid)
            if int(r) == teacher_of(vid) and t < first.get(vid, np.inf):
                first[vid] = t
    # logged span of each teach sequence (robot + run = vid >> 16), by minor id
    by_seq = {}
    for vid, t in first.items():
        by_seq.setdefault(vid >> 16, []).append((vid & 0xFFFF, t))
    seq_info = {}
    for seq, mt in by_seq.items():
        mt.sort()
        minors = np.array([m for m, _ in mt])
        later_min = np.minimum.accumulate(np.array([t for _, t in mt])[::-1])[::-1]   # earliest time at or after
        seq_info[seq] = (minors, later_min)

    out = {}
    src = {"taught (log)": 0, "between logged (filled)": 0, "taught (stamp)": 0,
           "before recording": 0, "after recording": 0}
    for vid in vids:
        vid = int(vid)
        if vid in first:
            out[vid] = first[vid]; src["taught (log)"] += 1
            continue
        if vid in stamps and t_lo - 86400 <= stamps[vid] <= t_hi + 3600:
            st = stamps[vid]
            out[vid] = st if st >= t_lo else -np.inf
            src["taught (stamp)" if st >= t_lo else "before recording"] += 1
            continue
        info = seq_info.get(vid >> 16)
        if info is None:                               # no vertex of this run was logged being taught
            out[vid] = -np.inf; src["before recording"] += 1
            continue
        minors, later_min = info
        m = vid & 0xFFFF
        if m < minors[0]:                              # before the teacher's logs start
            out[vid] = -np.inf; src["before recording"] += 1
        elif m > minors[-1]:                           # taught after the teacher's logs end: never shown
            out[vid] = np.inf; src["after recording"] += 1
        else:                                          # skipped between two logged vertices
            out[vid] = float(later_min[np.searchsorted(minors, m)]); src["between logged (filled)"] += 1
    print("  graph vertices: " + ", ".join(f"{k} {n}" for k, n in src.items() if n))
    return out


def map_check(ev, out, args):
    """One still: every vertex of the teach graph (and every visited vertex) over the basemap."""
    pos = read_positions(args.positions)
    geo = make_geo(args)
    if geo is None:
        print("  map_check needs --origin LAT LON THETA")
        return
    pos = to_mercator_dict(pos, geo)
    bg = np.array(list(pos.values()))
    fig, ax = plt.subplots(figsize=(9, 7.5), dpi=150)
    draw_map(ax, bg, geo, args, graph=False)
    for r in sorted({teacher_of(v) for v in pos}):
        xy = np.array([p for v, p in pos.items() if teacher_of(v) == r])
        ax.plot(xy[:, 0], xy[:, 1], ".", ms=1.5, color=color(r), alpha=0.6, zorder=1, label=f"taught by {name(r)}")
    root = geo.to_mercator([[0.0, 0.0]])[0]
    ax.plot(*root, marker="*", ms=14, color="red", mec="black", zorder=5, label="root vertex")
    if "vertex" in ev:
        for r, d in ev["vertex"].groupby("robot"):
            xy = np.array([pos[int(v)] for v in d["vid"].astype("uint64") if int(v) in pos])
            if len(xy):
                ax.plot(xy[:, 0], xy[:, 1], ".", ms=3, color=color(r), zorder=3, label=f"visited by {name(r)}")
    ax.legend(loc="upper right", fontsize=8)
    ax.set_title(f"map check: root ({geo.lat:.6f}, {geo.lon:.6f}), theta {math_deg(geo.theta):.1f} deg CCW from east")
    path = os.path.join(out, "map_check.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote map_check.png")


def math_deg(rad):
    return float(np.degrees(rad))


def strip_positions(vids):
    """No positions file: distance along each teach sequence, sequences laid end to end."""
    vids = [int(v) for v in vids]
    seqs = sorted({v >> 16 for v in vids})
    # include every vertex between a run's first and last logged one (unlogged ones were passed, not absent)
    full = []
    for s in seqs:
        ms = [v & 0xFFFF for v in vids if v >> 16 == s]
        full += [(s << 16) | m for m in range(min(ms), max(ms) + 1)]
    vids = full
    offset, base = 0.0, {}
    for s in seqs:
        base[s] = offset
        offset += (max(v & 0xFFFF for v in vids if v >> 16 == s) + 20) * MINOR_SPACING
    return {v: (base[v >> 16] + (v & 0xFFFF) * MINOR_SPACING, 0.0) for v in vids}


def animate(ev, out, args, with_flows):
    stem = "anim4_transfers" if with_flows else "anim3_positions"
    if "vertex" not in ev or not len(ev["vertex"]):
        print(f"  skipped {stem}: no vertex events")
        return
    v = ev["vertex"].sort_values("t")
    data = {int(r): (d["t"].to_numpy(), d["vid"].astype("uint64").to_numpy()) for r, d in v.groupby("robot")}
    all_vids = np.unique(v["vid"].astype("uint64"))
    strip = not args.positions
    pos = read_positions(args.positions) if args.positions else strip_positions(all_vids)
    stamps = read_stamps(args.positions) if args.positions else {}
    geo = make_geo(args) if not strip else None
    if geo is not None:
        pos = to_mercator_dict(pos, geo)               # root-frame metres -> Web Mercator
    lanes = {r: k for k, r in enumerate(sorted(data))}

    # vertex events are logged only when the vertex changes, so a parked robot logs nothing:
    # keep each robot visible at its last vertex until its log ends (or a log gap > --gap)
    logw = log_windows(ev)
    tracks, spans = {}, []
    for r, (t, vids) in data.items():
        tc, xy = track(t, vids, pos)
        if strip and len(xy):
            xy = xy.copy()
            xy[:, 1] = -lanes[r]
        lo, hi = logw.get(r, (t[0], t[-1]))
        alive = ev_times(ev, r)
        sp = [(max(a, t[0]), b) for a, b in active_spans(alive, args.gap) if b >= t[0]]
        tracks[r] = (tc, xy, sp)
        spans += sp

    fl = flows(ev, 1.0) if with_flows else None
    dt = args.speed / args.fps
    segments = merge(spans, pad=1.0)
    frames, jumps = [], {}
    for k, (a, b) in enumerate(segments):
        if k:
            jumps[len(frames)] = a - segments[k - 1][1]
        frames += list(np.arange(a, b, dt))
    if args.max_frames and len(frames) > args.max_frames:
        frames = frames[:args.max_frames]
    print(f"  {stem}: {len(frames)} frames ({len(frames) / args.fps:.0f} s of video at x{args.speed:g})")

    bg = np.array(list(pos.values()))
    t_lo = min(d["t"].min() for d in ev.values())
    t_hi = max(d["t"].max() for d in ev.values())
    live = not args.static_graph
    if strip:
        fig, ax = plt.subplots(figsize=(12, 1.8 + 1.0 * len(lanes)), dpi=110)
        x0, x1 = bg[:, 0].min(), bg[:, 0].max()
        for r, k in lanes.items():                    # live: no lane bar, it would show the route ahead
            ax.axhline(-k, color="#f2f2f2" if live else "#dddddd", lw=0.8 if live else 6, zorder=0)
        ax.set_xlim(x0 - 10, x1 + 10)
        ax.set_ylim(-len(lanes) + 0.4, 1.6)                     # headroom for the transfer list
        ax.set_yticks([-k for k in lanes.values()], [name(r) for r in lanes])
        ax.set_xlabel("distance along the teach sequences (m, from vertex ids; schematic)")
    else:
        fig, ax = plt.subplots(figsize=(9, 7.5), dpi=110)
        draw_map(ax, bg, geo, args, graph=not live)

    # the graph, drawn as it is taught and coloured by the robot that taught it
    graph = []          # (artist, reveal times sorted, xs, ys) per teaching robot (and per lane in strip mode)
    if live:
        rt = reveal_times(ev, pos.keys(), stamps, t_lo, t_hi)
        rows = [(-k) for k in lanes.values()] if strip else [None]
        on_imagery = geo is not None and args.basemap != "none"
        import matplotlib.patheffects as pe
        outline = [pe.withStroke(linewidth=3, foreground="black", alpha=0.6)] if on_imagery else None
        for r in sorted({teacher_of(v) for v in pos}):
            vs = sorted((rt[v], v) for v in pos if teacher_of(v) == r)
            times = np.array([t for t, _ in vs])
            xy = np.array([pos[v] for _, v in vs])
            before = np.isneginf(times)
            keep = ~np.isposinf(times)                 # taught after the recording: never drawn
            times, xy, before = times[keep], xy[keep], before[keep]
            for y in rows:
                ys = xy[:, 1] if y is None else np.full(len(xy), float(y))
                if before.any():                       # taught before the recording: there from the start
                    ax.plot(xy[before, 0], ys[before], ".", ms=1.5 if y is None else 3, color=color(r),
                            alpha=0.5, zorder=1, path_effects=outline)
                art, = ax.plot([], [], ".", ms=2.5 if y is None else 4, color=color(r), alpha=0.95, zorder=1, path_effects=outline)
                graph.append((art, times[~before], xy[~before, 0], ys[~before]))
        ax.text(0.99, 0.01, "graph drawn as it is taught; colour = robot that taught it",
                transform=ax.transAxes, ha="right", va="bottom", fontsize=7,
                color="white" if (geo is not None and args.basemap != "none") else "#555", zorder=6)

    dots, trails, labels = {}, {}, {}
    for r in tracks:
        trails[r], = ax.plot([], [], "-", color=color(r), lw=2, alpha=0.5, zorder=2)
        dots[r], = ax.plot([], [], "o", color=LINE_COLORS.get(r, "#999"), ms=11, mec="black", mew=0.8,
                           zorder=4, label=name(r))
        labels[r] = ax.text(0, 0, "", fontsize=8, zorder=5)
        if not strip:                                   # readable on imagery and on white
            import matplotlib.patheffects as pe
            labels[r].set_path_effects([pe.withStroke(linewidth=2.5, foreground="white")])
    if not strip:
        ax.legend(loc="upper left", fontsize=8, framealpha=0.85)   # transfer list sits upper right
    title = ax.set_title("00:00:00 UTC", pad=14)
    note = ax.text(0.0, 1.01, "", transform=ax.transAxes, fontsize=8, color="#b00020")
    flow_box = ax.text(0.99, 0.97, "", transform=ax.transAxes, ha="right", va="top", fontsize=8,
                       family="monospace", zorder=6,
                       bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="#cccccc", alpha=0.85))
    flow_box.set_visible(False)
    arrows = []
    win = max(dt, args.flow_window)                      # seconds of transfer averaged per frame
    if with_flows:
        f_t = fl["t"].to_numpy()
        peak = fl.groupby("t")["bytes"].max().max() if len(fl) else 1.0
    note_until = [-1]

    def draw(i):
        T = frames[i]
        for art, times, xs, ys in graph:
            n = np.searchsorted(times, T, side="right")
            art.set_data(xs[:n], ys[:n])
        title.set_text(f"{'Robot positions and transfers' if with_flows else 'Robot positions'}   "
                       f"{fmt_t(T)} UTC   (x{args.speed:g})")
        if i in jumps:
            note_until[0] = i + 2 * args.fps
            note.set_text(f"no data for {jumps[i] / 60:.1f} min, skipped")
        if i > note_until[0]:
            note.set_text("")
        here = {}
        for r, (tc, xy, sp) in tracks.items():
            p = position_at(T, tc, xy) if any(a - 1 <= T <= b + 1 for a, b in sp) and len(tc) else None
            if p is None:
                dots[r].set_data([], [])
                trails[r].set_data([], [])
                labels[r].set_text("")
                continue
            here[r] = p
            m = (tc >= T - args.trail) & (tc <= T)
            pts = np.vstack([xy[m], p])
            trails[r].set_data(pts[:, 0], pts[:, 1])
            dots[r].set_data([p[0]], [p[1]])
            if not strip:
                x0, x1 = ax.get_xlim()
                off = 0.015 * (x1 - x0)
                right = p[0] > x0 + 0.75 * (x1 - x0)
                labels[r].set_position((p[0] - off if right else p[0] + off, p[1] + off))
                labels[r].set_horizontalalignment("right" if right else "left")
                # labels[r].set_text(ROBOT_NAMES.get(r, str(r)))

        if with_flows:
            for a in arrows:
                a.remove()
            arrows.clear()
            m = (f_t > T - win) & (f_t <= T)
            cur = fl[m].groupby(["src", "dst"])["bytes"].sum() / win      # B/s
            lines = []
            for (s, d), rate in cur.sort_values(ascending=False).items():
                if rate <= 0:
                    continue
                lines.append(f"{ROBOT_NAMES.get(s, s):>12s} -> {ROBOT_NAMES.get(d, d):<12s} {rate / MB:5.2f} MB/s")
                if s in here and d in here and not np.allclose(here[s], here[d]):
                    w = 1.0 + 7.0 * min(1.0, rate / max(peak / win, 1.0))
                    arr = FancyArrowPatch(tuple(here[s]), tuple(here[d]), arrowstyle="-|>",
                                          mutation_scale=10 + 2 * w, lw=w, color=color(s), alpha=0.75,
                                          connectionstyle="arc3,rad=0.15", zorder=3,
                                          shrinkA=8, shrinkB=8)
                    ax.add_patch(arr)
                    arrows.append(arr)
            flow_box.set_text("\n".join(lines[:6]))
            flow_box.set_visible(bool(lines))
        return []

    fig.tight_layout()
    if args.frame_at:                                    # one still instead of a video
        from datetime import datetime, timezone
        day = datetime.fromtimestamp(frames[0], timezone.utc).date()
        hh, mm, ss = (int(x) for x in args.frame_at.split(":"))
        T = datetime(day.year, day.month, day.day, hh, mm, ss, tzinfo=timezone.utc).timestamp()
        frames.append(T)
        frames.sort()
        draw(frames.index(T))
        path = os.path.join(out, f"{stem}_{args.frame_at.replace(':', '')}.png")
        fig.savefig(path, dpi=200, bbox_inches="tight")
        plt.close(fig)
        print(f"  wrote {os.path.basename(path)}")
        return
    anim = animation.FuncAnimation(fig, draw, frames=len(frames), blit=False)
    path = os.path.join(out, stem + (".gif" if args.gif else ".mp4"))
    writer = animation.PillowWriter(fps=args.fps) if args.gif else animation.FFMpegWriter(fps=args.fps, bitrate=3000)
    anim.save(path, writer=writer)
    plt.close(fig)
    print(f"  wrote {os.path.basename(path)}")


# ======================================================================= main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("logs", nargs="+", help="log files or directories (all robots, all rotated parts)")
    ap.add_argument("-o", "--out", default="figures")
    ap.add_argument("--positions", help="vid,x,y CSV from export_vertex_positions.py")
    ap.add_argument("--only", default="fig1,fig2,fig3,anim3,anim4",
                    help="comma list of fig1,fig2,fig3,anim3,anim4,map_check")
    ap.add_argument("--fmt", default="png,pdf", help="figure formats")
    ap.add_argument("--bin", type=float, default=10.0, help="rate bin for fig2 (s)")
    ap.add_argument("--piece_size", type=float, default=2 * 2**20, help="seeder PIECE_SIZE (bytes)")
    ap.add_argument("--speed", type=float, default=20.0, help="experiment seconds per video second")
    ap.add_argument("--fps", type=int, default=10)
    ap.add_argument("--trail", type=float, default=30.0, help="position trail (s)")
    ap.add_argument("--gap", type=float, default=30.0, help="skip periods with no position data longer than this (s)")
    ap.add_argument("--flow_window", type=float, default=5.0, help="anim4: average transfer rate over this many s")
    ap.add_argument("--max_frames", type=int, default=0, help="cap frames (for a quick preview)")
    ap.add_argument("--gif", action="store_true", help="write GIFs instead of MP4")
    ap.add_argument("--frame_at", help="HH:MM:SS (UTC): write one still of each animation at that time")
    ap.add_argument("--static_graph", action="store_true",
                    help="draw the whole teach graph from the first frame (grey) instead of as it is taught")
    g = ap.add_argument_group("satellite basemap (needs --positions)")
    g.add_argument("--origin", nargs=3, type=float, metavar=("LAT", "LON", "THETA"),
                   help="root vertex: latitude, longitude (deg) and heading THETA (rad, CCW from east), "
                        "as in VTR's MapInfo / graph_projection")
    g.add_argument("--map_scale", type=float, default=1.0, help="MapInfo scale (default 1)")
    g.add_argument("--rotate_deg", type=float, default=0.0, help="extra rotation to fine-tune alignment")
    g.add_argument("--flip_y", action="store_true", help="mirror the root frame's y axis (if the path is mirrored)")
    g.add_argument("--offset_m", nargs=2, type=float, default=(0.0, 0.0), metavar=("EAST", "NORTH"),
                   help="shift in metres to fine-tune alignment")
    g.add_argument("--basemap", default="Esri.WorldImagery",
                   help="contextily provider (Esri.WorldImagery, OpenStreetMap.Mapnik, ...) or 'none'")
    g.add_argument("--tile_cache", default=os.path.expanduser("~/.cache/ttr_tiles"),
                   help="tile cache directory (downloads once, re-renders offline)")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    only = set(args.only.split(","))
    fmts = args.fmt.split(",")

    ev = te.load(args.logs)
    if not ev:
        sys.exit("no EVT lines found")
    STARTED.update(robots_with_start(args.logs))
    partial = sorted(set(ev["torrent"]["robot"]) - STARTED) if "torrent" in ev else []
    if partial:
        print(f"note: logs for robot(s) {partial} start mid-run; transfer totals cover only the logged window\n")
    print(te.summary(ev))
    t0 = min(d["t"].min() for d in ev.values())
    t1 = max(d["t"].max() for d in ev.values())
    print(f"\nrun: {fmt_t(t0)} -> {fmt_t(t1)} UTC ({(t1 - t0) / 60:.1f} min); writing to {args.out}/")

    if "xfer" in ev:
        flows(ev, 1.0).to_csv(os.path.join(args.out, "flows_1s.csv"), index=False)
        um = upload_matrix(ev)
        um.to_csv(os.path.join(args.out, "upload_matrix.csv"), index=False)
        print("\nupload matrix (MB, sender's count vs receiver's count):")
        print(um.to_string(index=False))
    print()
    if "fig1" in only:
        fig1_maps(ev, t0, args.out, fmts, args.piece_size)
    if "fig2" in only:
        tot = fig2_bandwidth(ev, t0, t1, args.out, fmts, args.bin)
        print("  totals received: " + ", ".join(f"{k} {v / MB:,.2f} MB" for k, v in tot.items()))
    if "fig3" in only and "xfer" in ev:
        fig3_upload(ev, t0, args.out, fmts)
    if "map_check" in only:
        map_check(ev, args.out, args)
    if "anim3" in only:
        animate(ev, args.out, args, with_flows=False)
    if "anim4" in only and "xfer" in ev:
        animate(ev, args.out, args, with_flows=True)


if __name__ == "__main__":
    main()