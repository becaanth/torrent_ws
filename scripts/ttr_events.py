#!/usr/bin/env python3
"""
ttr_events.py -- load the structured EVT lines from every robot's orchestrator logs.

    from ttr_events import load
    ev = load("runs/convoy_3/")          # dir (searched recursively) or list of log files; rotated parts included
    ev["peer"]       # DataFrame: t, robot, ih, origin, role, peer, addr, dl, ul, dl_rate, ul_rate
    ev["torrent"]    # t, robot, ih, origin, role, state, dl, ul, dl_wire, ul_wire, dl_rate, ul_rate, have, pieces, peers
    ev["xfer"]       # t, robot, dir, peer, addr, bytes, blocks     (EXACT cumulative bytes per peer, 1 Hz)
    ev["piece"]      # t, robot, ih, file, origin, bytes          (first completion of each file = new data)
    ev["metadata"]   # t, robot, ih, origin, bytes, pieces, files
    ev["gossip_tx"]  # t, robot, origin, seq, ih, bytes, kind     (publish | forward)
    ev["gossip_rx"]  # t, robot, origin, seq, ih, bytes, via_ip    (forwarding robot's address)
    ev["snapshot"]   # t, robot, seq, ih, bytes, metadata          (bytes = torrent payload size)
    ev["chunk"]      # t, robot, file, sid, bytes                  (piece file written by the deconstitutor)
    ev["vertex"]     # t, robot, vid                              (only when the localized vertex changes)
    ev["ingest"]     # t, robot, file, origin

"robot" is the robot whose log the line came from (from the orchestrator's "init with id N" line).
Counters in "peer" and "torrent" restart when a torrent is replaced or a connection reconnects;
use transfers() to turn them into per-interval byte deltas that handle the resets.

CLI:  python3 ttr_events.py <dir or logs...>    -> summary of what was recorded per robot
"""
import glob
import json
import os
import re
import sys
from collections import defaultdict

import pandas as pd

EVT = re.compile(r"\]: EVT (\{.*\})\s*$")
ROBOT = re.compile(r"init with id (\d+)")


def _files(paths):
    if isinstance(paths, str):
        paths = [paths]
    out = []
    for p in paths:
        if os.path.isdir(p):
            out += glob.glob(os.path.join(p, "**", "*.log*"), recursive=True)
        else:
            out += glob.glob(p)
    return sorted(set(out))


def _robot_of(path, cache):
    """Robot id of a log file; rotated parts (.log.N) share the base file's id."""
    base = re.sub(r"\.log\.\d+$", ".log", path)
    if base in cache:
        return cache[base]
    rid = None
    for candidate in sorted(glob.glob(base + "*"), key=lambda f: (not f.endswith(".log"), f)):
        with open(candidate, errors="replace") as f:
            for _, line in zip(range(200), f):
                m = ROBOT.search(line)
                if m:
                    rid = int(m.group(1))
                    break
        if rid is not None:
            break
    if rid is None:  # e.g. convoy_experiment_1_20261022_214712.log -> 1
        m = re.search(r"_(\d+)_\d{8}_\d{6}\.log", base)
        rid = int(m.group(1)) if m else None
    cache[base] = rid
    return rid


def load(paths):
    rows, cache = defaultdict(list), {}
    for path in _files(paths):
        robot = _robot_of(path, cache)
        with open(path, errors="replace") as f:
            for line in f:
                if "EVT {" not in line:
                    continue
                m = EVT.search(line)
                if not m:
                    continue
                try:
                    d = json.loads(m.group(1))
                except json.JSONDecodeError:
                    continue      # line cut by a crash or rotation
                d["robot"] = robot
                rows[d.pop("k")].append(d)
    out = {}
    for kind, r in rows.items():
        df = pd.DataFrame(r)
        for c in ("origin", "peer"):        # robot ids arrive as str (env, folder names) or int
            if c in df:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
        df = df.drop_duplicates().sort_values("t", kind="stable").reset_index(drop=True)
        cols = ["t", "robot"] + [c for c in df.columns if c not in ("t", "robot")]
        out[kind] = df[cols]
    return out


def transfers(peer_df, value="dl"):
    """
    Per-sample byte deltas from the cumulative per-connection counters in ev["peer"].
    A counter that goes down means a new connection (or torrent): count from zero again.
    Returns peer_df rows with a 'bytes' column = bytes moved since the previous sample of
    the same (robot, ih, addr) series. With value="dl", that is bytes robot received from `peer`.
    """
    df = peer_df.sort_values("t").copy()
    key = ["robot", "ih", "addr", "peer"]
    prev = df.groupby(key, dropna=False)[value].shift()
    delta = df[value] - prev
    df["bytes"] = delta.where(delta >= 0, df[value]).fillna(df[value])
    return df


def flows(xfer_df, period=1.0):
    """
    Exact data flow between robots from ev["xfer"] (block-level counters, never reset).
    Returns rows (t, src, dst, bytes) per `period` seconds, from the receiver's side
    (dir == "down": robot received `bytes` from `peer`). Use the "up" side to cross-check.
    """
    d = xfer_df[xfer_df["dir"] == "down"].sort_values("t").copy()
    d["delta"] = d.groupby(["robot", "peer", "addr"], dropna=False)["bytes"].diff().fillna(d["bytes"])
    d["t"] = (d["t"] // period) * period
    out = d.groupby(["t", "peer", "robot"], dropna=False)["delta"].sum().reset_index()
    return out.rename(columns={"peer": "src", "robot": "dst", "delta": "bytes"})


def summary(ev):
    lines = []
    for kind in sorted(ev):
        df = ev[kind]
        per_robot = df.groupby("robot").size().to_dict()
        span = (df["t"].max() - df["t"].min()) / 60 if len(df) > 1 else 0
        lines.append(f"{kind:10s} {len(df):7d} events over {span:6.1f} min   per robot {per_robot}")
    return "\n".join(lines)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    ev = load(sys.argv[1:])
    if not ev:
        sys.exit("no EVT lines found -- logs from before the structured logging change?")
    print(summary(ev))
    if "peer" in ev and ev["peer"]["peer"].isna().any():
        n = int(ev["peer"]["peer"].isna().sum())
        print(f"\nnote: {n} peer samples have no robot id (peer_fingerprint not set on that peer?)")