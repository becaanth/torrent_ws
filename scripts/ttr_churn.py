#!/usr/bin/env python3
"""
ttr_churn.py <repeater orchestrator log> [more logs ...] [--csv out.csv]

Measure how long a repeater stalls each time a robot it downloads from publishes a new
snapshot (torrent churn). Reads the orchestrator log format:
  [2026-10-22 19:18:23.355] [INFO] [GossiperThread] [orchestrator.py:157]: handle_item_updated, robot id 0, seq 52

For every snapshot switch (handle_item_updated) it finds, until the next switch:
  to_metadata   : switch -> "metadata received for torrent" (gossip already arrived; rejoin + ut_metadata)
  to_first_file : switch -> first "file N completed" on the NEW torrent (includes the existing-file re-check)
  stall         : last file completed BEFORE the switch -> first file completed AFTER it
  stale         : alerts for the replaced torrent that arrived after the switch (wasted work)
Note: log timestamps are when the peer thread HANDLED an alert. With the old fixed-sleep
peer loop that is up to one poll period after the event; with wait_for_alert it is ~immediate.

Multiple robots publishing are kept separate (switches are tracked per robot id; the
torrent hash that follows a switch is attributed to that robot).
"""
import argparse
import csv
import re
import statistics as st
import sys
from datetime import datetime

LINE = re.compile(r"^\[(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3})\] \[\w+\] \[[^\]]*\] \[[^\]]*\]: (.*)$")
SWITCH = re.compile(r"handle_item_updated, robot id (\S+), seq (\d+)")
NEW_ITEM = re.compile(r"handle_new_item from robot id (\S+)")
METADATA = re.compile(r"metadata received for torrent: (\w+)")
FILE_DONE = re.compile(r"file (\d+) completed on torrent: (\w+)")
STALE = re.compile(r"ignoring (\w+): torrent was replaced")
STATE = re.compile(r": state changed to: (\w+)")


def parse(paths):
    ev = []
    for p in paths:
        with open(p, errors="replace") as f:
            for line in f:
                m = LINE.match(line)
                if not m:
                    continue
                t = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S.%f").timestamp()
                msg = m.group(2)
                if (g := SWITCH.search(msg)):
                    ev.append((t, "switch", g.group(1), int(g.group(2))))
                elif (g := NEW_ITEM.search(msg)):
                    ev.append((t, "switch", g.group(1), 0))
                elif (g := METADATA.search(msg)):
                    ev.append((t, "meta", g.group(1), None))
                elif (g := FILE_DONE.search(msg)):
                    ev.append((t, "file", g.group(2), int(g.group(1))))
                elif STALE.search(msg):
                    ev.append((t, "stale", None, None))
                elif (g := STATE.search(msg)):
                    ev.append((t, "state", g.group(1), None))
    ev.sort(key=lambda e: e[0])
    return ev


def analyse(ev):
    switches = [i for i, e in enumerate(ev) if e[1] == "switch"]
    files = [e for e in ev if e[1] == "file"]
    rows = []
    for k, i in enumerate(switches):
        t0, _, robot, seq = ev[i]
        t_next = ev[switches[k + 1]][0] if k + 1 < len(switches) else float("inf")
        window = [e for e in ev[i + 1:] if e[0] < t_next]

        meta = next((e for e in window if e[1] == "meta"), None)
        new_hash = meta[2] if meta else None
        # files reported while the new torrent re-checks existing data are not new downloads:
        # only count completions after it reaches the "downloading" state that follows its metadata
        dl = next((e for e in window if e[1] == "state" and e[2] == "downloading"
                   and meta is not None and e[0] >= meta[0]), None)
        first_new = next((e for e in window if e[1] == "file" and dl is not None and e[0] > dl[0]
                          and (new_hash is None or e[2] == new_hash)), None)
        last_before = next((e for e in reversed(files) if e[0] < t0), None)
        stale = sum(1 for e in window if e[1] == "stale")
        rows.append({
            "t": t0, "robot": robot, "seq": seq, "hash": new_hash or "",
            "to_metadata": (meta[0] - t0) if meta else None,
            "to_downloading": (dl[0] - t0) if dl else None,
            "to_first_file": (first_new[0] - t0) if first_new else None,
            "stall": (first_new[0] - last_before[0]) if (first_new and last_before) else None,
            "interval": (t_next - t0) if t_next != float("inf") else None,
            "files_in_window": sum(1 for e in window if e[1] == "file"),
            "stale": stale,
        })
    return rows, files


def fmt(x):
    return "    -" if x is None else f"{x:5.1f}"


def summary(name, vals):
    v = [x for x in vals if x is not None]
    if not v:
        return f"  {name:14s} no data"
    v.sort()
    p90 = v[min(len(v) - 1, int(0.9 * len(v)))]
    return f"  {name:14s} median {st.median(v):5.1f} s   p90 {p90:5.1f} s   max {v[-1]:5.1f} s   (n={len(v)})"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("logs", nargs="+")
    ap.add_argument("--csv", help="write one row per switch here")
    args = ap.parse_args()

    ev = parse(args.logs)
    if not ev:
        sys.exit("no recognised log lines (expected the orchestrator log format)")
    rows, files = analyse(ev)

    t_start, t_end = ev[0][0], ev[-1][0]
    print(f"{len(rows)} snapshot switches, {len(files)} files completed over {(t_end - t_start)/60:.1f} min\n")
    print(f"{'time':8s} {'robot':>5s} {'seq':>4s} {'every':>6s} {'->meta':>6s} {'->dl':>6s} {'->file':>6s} {'stall':>6s} {'files':>5s} {'stale':>5s}")
    for r in rows:
        print(f"{datetime.fromtimestamp(r['t']).strftime('%H:%M:%S')} {r['robot']:>5s} {r['seq']:4d} "
              f"{fmt(r['interval'])}  {fmt(r['to_metadata'])}  {fmt(r['to_downloading'])}  {fmt(r['to_first_file'])}  {fmt(r['stall'])} "
              f"{r['files_in_window']:5d} {r['stale']:5d}")

    print("\nper switch:")
    print(summary("interval", [r["interval"] for r in rows]))
    print(summary("->metadata", [r["to_metadata"] for r in rows]))
    print(summary("->downloading", [r["to_downloading"] for r in rows]))
    print(summary("->first file", [r["to_first_file"] for r in rows]))
    print(summary("stall", [r["stall"] for r in rows]))

    # dead time: from each switch until the new torrent delivers its first file (capped at the next switch)
    dead = sum(min(r["to_first_file"] if r["to_first_file"] is not None else (r["interval"] or 0),
                   r["interval"] if r["interval"] is not None else float("inf")) for r in rows)
    span = t_end - t_start
    print(f"\ntime spent re-joining after a switch: {dead:.0f} s of {span:.0f} s ({100*dead/max(span,1):.0f}%)")
    if span > dead and files:
        print(f"file events/min {60*len(files)/span:.1f} (includes files re-reported by each re-check)")

    # does the re-join get slower as the map grows? (existing-file re-check is O(files on disk))
    tf = [(r["files_in_window"], r["to_first_file"]) for r in rows if r["to_first_file"] is not None]
    if len(rows) >= 6:
        half = len(rows) // 2
        a = [r["to_first_file"] for r in rows[:half] if r["to_first_file"] is not None]
        b = [r["to_first_file"] for r in rows[half:] if r["to_first_file"] is not None]
        if a and b:
            print(f"->first file, first half of run median {st.median(a):.1f} s vs second half {st.median(b):.1f} s"
                  " (growth points at the existing-file re-check)")

    if args.csv:
        with open(args.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"\nwrote {args.csv}")


if __name__ == "__main__":
    main()