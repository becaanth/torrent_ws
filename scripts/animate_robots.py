#!/usr/bin/env python3
"""
animate_robots.py -- animation 3: where each robot was on the teach graph over time.

    python3 animate_robots.py <logs or dirs ...> --positions vertex_positions.csv -o robots.mp4

Inputs
  logs        every robot's orchestrator logs (dirs are searched; rotated parts .log.N included).
              Reads EVT "vertex" events; for logs from before structured logging it falls back to
              the "localized to vertex" lines from tr_listener.
  --positions vid,x,y CSV from export_vertex_positions.py (run on the teach graph). Without it the
              animation is a strip chart: distance along the teach sequence (minor id x ~0.3 m)
              on x, one lane per robot -- a schematic, labelled so.

Robot id comes from the log's "init with id N" line, else the filename (<posegraph>_<id>_<date>_<time>.log).
Clock: EVT times are epoch seconds; legacy lines are read as UTC (the robots log in UTC).
Stretches where no robot has data for longer than --gap seconds are skipped (the clock jumps,
and the title says so), so partial or rotated logs still give a watchable animation.
"""
import argparse
import csv
import glob
import json
import os
import re
from datetime import datetime, timezone

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import animation
import numpy as np

ROBOT_NAMES = {0: "mr_green", 1: "prof_plum", 2: "col_mustard", 3: "mrs_peacock"}
ROBOT_COLORS = ["#3cb44b", "#911eb4", "#ffe119", "#4363d8", "#e6194b", "#f58231", "#bfef45", "#42d4f4",
                "#f032e6", "#fabebe", "#ffd8b1", "#fffac8", "#aaffc3", "#a9a9a9", "#ffffff", "#000000"]
MINOR_SPACING = 0.3   # m, schematic layout only (vertex_test max_translation)

LINE_TIME = re.compile(r"^\[(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3})\]")
LEGACY = re.compile(r"tr_listener\.py:\d+\]: localized to vertex (\d+)")
EVT = re.compile(r"\]: EVT (\{.*\})\s*$")
INIT = re.compile(r"init with id (\d+)")
NAME_ID = re.compile(r"_(\d+)_\d{8}_\d{6}\.log")


# ---------------------------------------------------------------- input
def log_files(paths):
    out = []
    for p in paths:
        out += glob.glob(os.path.join(p, "**", "*.log*"), recursive=True) if os.path.isdir(p) else glob.glob(p)
    return sorted(set(out))


def robot_id(path):
    with open(path, errors="replace") as f:
        for _, line in zip(range(500), f):
            if (m := INIT.search(line)):
                return int(m.group(1))
    m = NAME_ID.search(os.path.basename(path))
    return int(m.group(1)) if m else None


def read_vertices(paths):
    """-> {robot: (t_samples, vid_samples)} using EVT vertex events if a robot has any, else legacy lines."""
    evt, legacy = {}, {}
    for path in log_files(paths):
        rid = robot_id(path)
        if rid is None:
            print(f"skipping {path}: robot id unknown")
            continue
        with open(path, errors="replace") as f:
            for line in f:
                if "EVT {" in line and '"vertex"' in line and (m := EVT.search(line)):
                    d = json.loads(m.group(1))
                    evt.setdefault(rid, []).append((d["t"], int(d["vid"])))
                elif "localized to vertex" in line and (m := LEGACY.search(line)) and (tm := LINE_TIME.match(line)):
                    t = datetime.strptime(tm.group(1), "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc).timestamp()
                    legacy.setdefault(rid, []).append((t, int(m.group(1))))
    out = {}
    for rid in sorted(set(evt) | set(legacy)):
        rows = sorted(set(evt.get(rid) or legacy[rid]))
        out[rid] = (np.array([r[0] for r in rows]), np.array([r[1] for r in rows], dtype=np.uint64))
        src = "EVT" if rid in evt else "legacy"
        print(f"robot {rid} ({ROBOT_NAMES.get(rid, '?')}): {len(rows)} samples from {src} lines, "
              f"{fmt_t(rows[0][0])} -> {fmt_t(rows[-1][0])}")
    return out


def read_positions(path):
    pos = {}
    with open(path) as f:
        for r in csv.DictReader(f):
            pos[int(r["vid"])] = (float(r["x"]), float(r["y"]))
    return pos


def schematic_positions(vids):
    """No positions file: distance along the teach sequence from the minor id; y is set per robot lane."""
    return {int(v): ((int(v) & 0xFFFF) * MINOR_SPACING, 0.0) for v in vids}


def fmt_t(t):
    return datetime.fromtimestamp(t, timezone.utc).strftime("%H:%M:%S")


# ---------------------------------------------------------------- timeline
def active_spans(t, gap):
    """Intervals where samples are no more than `gap` s apart."""
    if len(t) == 0:
        return []
    breaks = np.where(np.diff(t) > gap)[0]
    starts = np.r_[t[0], t[breaks + 1]]
    ends = np.r_[t[breaks], t[-1]]
    return list(zip(starts, ends))


def merge(spans, pad):
    spans = sorted((a - pad, b + pad) for a, b in spans)
    out = []
    for a, b in spans:
        if out and a <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], b))
        else:
            out.append((a, b))
    return out


def track(t, vids, pos, hold=2.0):
    """Change points (time, xy) for one robot; positions interpolate between changes < hold s apart."""
    keep = np.r_[True, vids[1:] != vids[:-1]]
    tc, vc = t[keep], vids[keep]
    xy, ok = [], []
    for v in vc:
        p = pos.get(int(v))
        ok.append(p is not None)
        xy.append(p if p is not None else (np.nan, np.nan))
    missing = len(ok) - sum(ok)
    if missing:
        print(f"   {missing} of {len(ok)} visited vertices have no position (not in the positions CSV)")
    tc, xy = tc[np.array(ok)], np.array(xy)[np.array(ok)]
    return tc, xy


def position_at(T, tc, xy, hold=2.0):
    i = np.searchsorted(tc, T, side="right") - 1
    if i < 0:
        return None
    if i + 1 < len(tc) and tc[i + 1] - tc[i] < hold:      # moving: glide to the next vertex
        a = (T - tc[i]) / (tc[i + 1] - tc[i])
        return xy[i] + a * (xy[i + 1] - xy[i])
    return xy[i]


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("logs", nargs="+")
    ap.add_argument("--positions", help="vid,x,y CSV from export_vertex_positions.py")
    ap.add_argument("-o", "--out", default="robots.mp4", help=".mp4 (ffmpeg) or .gif")
    ap.add_argument("--fps", type=int, default=10)
    ap.add_argument("--speed", type=float, default=10.0, help="seconds of experiment per second of video")
    ap.add_argument("--trail", type=float, default=30.0, help="trail length, seconds of experiment")
    ap.add_argument("--gap", type=float, default=30.0, help="skip stretches with no data longer than this (s)")
    args = ap.parse_args()

    data = read_vertices(args.logs)
    if not data:
        raise SystemExit("no vertex data found")

    all_vids = np.unique(np.concatenate([v for _, v in data.values()]))
    schematic = not args.positions
    if args.positions:
        pos = read_positions(args.positions)
        background = np.array(list(pos.values()))
        layout = "teach graph"
    else:
        pos = schematic_positions(all_vids)
        background = np.array([pos[int(v)] for v in all_vids])
        layout = f"schematic: vertices by minor id, {MINOR_SPACING} m apart (no --positions given)"

    lanes = {rid: k for k, rid in enumerate(sorted(data))}
    tracks, spans = {}, []
    for rid, (t, vids) in data.items():
        tc, xy = track(t, vids, pos)
        if schematic and len(xy):
            xy = xy.copy(); xy[:, 1] = -lanes[rid]          # one lane per robot
        tracks[rid] = (tc, xy, active_spans(t, args.gap))
        spans += tracks[rid][2]

    # frame clock: run through the periods where anyone has data, jump over the rest
    dt = args.speed / args.fps
    segments = merge(spans, pad=1.0)
    frames, jumps = [], {}
    for k, (a, b) in enumerate(segments):
        if k:
            jumps[len(frames)] = a - segments[k - 1][1]
        frames += list(np.arange(a, b, dt))
    print(f"{len(frames)} frames ({len(frames)/args.fps:.0f} s of video), {len(segments)} active period(s)")

    # ---- figure
    if schematic:
        fig, ax = plt.subplots(figsize=(12, 1.6 + 0.9 * len(lanes)), dpi=110)
        x0, x1 = background[:, 0].min(), background[:, 0].max()
        for rid, k in lanes.items():
            ax.plot([x0, x1], [-k, -k], "-", color="#d8d8d8", lw=6, solid_capstyle="round", zorder=1)
        ax.set_xlim(x0 - 10, x1 + 10)
        ax.set_ylim(-len(lanes) + 0.4, 0.8)
        ax.set_yticks([-k for k in lanes.values()])
        ax.set_yticklabels([f"{ROBOT_NAMES.get(r, 'robot')} ({r})" for r in lanes])
        ax.set_xlabel("distance along the teach sequence (m, from vertex minor id)")
        ax.text(0.0, 1.02, layout, transform=ax.transAxes, fontsize=8, color="#666666")
    else:
        fig, ax = plt.subplots(figsize=(9, 7), dpi=110)
        ax.plot(background[:, 0], background[:, 1], ".", color="#c8c8c8", ms=2, zorder=1, label="teach graph vertices")
        span = max(np.ptp(background[:, 0]), np.ptp(background[:, 1]), 10.0)
        pad = 0.06 * span
        ax.set_xlim(background[:, 0].min() - pad, background[:, 0].max() + pad)
        ax.set_ylim(background[:, 1].min() - pad, background[:, 1].max() + pad)
        ax.set_aspect("equal")
        ax.set_xlabel("x (m)")
        ax.set_ylabel("y (m)")
        ax.text(0.01, 0.01, layout, transform=ax.transAxes, fontsize=8, color="#666666")

    artists = {}
    for rid in tracks:
        c = ROBOT_COLORS[rid % len(ROBOT_COLORS)]
        name = f"{ROBOT_NAMES.get(rid, 'robot')} ({rid})"
        trail, = ax.plot([], [], "-", color=c, lw=2, alpha=0.5, zorder=2)
        dot, = ax.plot([], [], "o", color=c, ms=10, mec="black", mew=0.8, zorder=3, label=name)
        label = ax.text(0, 0, "", fontsize=8, zorder=4)
        artists[rid] = (trail, dot, label)
    if not schematic:
        ax.legend(loc="upper right", fontsize=8)
    title = ax.set_title("Robot positions   00:00:00 UTC   (x10)", pad=16 if schematic else 6)  # sized for layout
    jump_note = ax.text(1.0, 1.02 if schematic else 0.95, "", transform=ax.transAxes, ha="right",
                        fontsize=9, color="#b00020")
    note_until = [-1]

    def draw(i):
        T = frames[i]
        title.set_text(f"Robot positions   {fmt_t(T)} UTC   (x{args.speed:g})")
        if i in jumps:
            note_until[0] = i + 2 * args.fps
            jump_note.set_text(f"no data for {jumps[i]/60:.1f} min -- skipped")
        if i > note_until[0]:
            jump_note.set_text("")
        for rid, (tc, xy, rspans) in tracks.items():
            trail, dot, label = artists[rid]
            active = any(a - 1.0 <= T <= b + 1.0 for a, b in rspans)
            p = position_at(T, tc, xy) if active and len(tc) else None
            if p is None:
                trail.set_data([], []); dot.set_data([], []); label.set_text("")
                continue
            m = (tc >= T - args.trail) & (tc <= T)
            pts = np.vstack([xy[m], p])
            trail.set_data(pts[:, 0], pts[:, 1])
            dot.set_data([p[0]], [p[1]])
            if not schematic:                                   # lanes are already labelled
                x0, x1 = ax.get_xlim()
                off = 0.015 * (x1 - x0)
                right_half = p[0] > x0 + 0.75 * (x1 - x0)       # near the right edge: label on the left
                label.set_position((p[0] - off if right_half else p[0] + off, p[1] + off))
                label.set_horizontalalignment("right" if right_half else "left")
                label.set_text(ROBOT_NAMES.get(rid, str(rid)))
        return [title, jump_note] + [a for trio in artists.values() for a in trio]

    fig.tight_layout()
    anim = animation.FuncAnimation(fig, draw, frames=len(frames), blit=False)
    if args.out.endswith(".gif"):
        anim.save(args.out, writer=animation.PillowWriter(fps=args.fps))
    else:
        anim.save(args.out, writer=animation.FFMpegWriter(fps=args.fps, bitrate=2400))
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()