"""
measure_topo.py -- size of the per-piece topology annotations the seeder writes to the torrent metadata

For each deconstituted piece, mirrors MutableSeeder.create_snapshot: parse_chunk -> pack_topology,
and reports the x-topo size against the legacy x-vertices/x-edges msgpack, the piece file size,
and the real (pre-padding) data in it. Also checks the x-topo round trip (max xi quantization error).

Usage (from scripts/):
    python3 -m torrent.measure_topo -p <posegraph>              # $VTRTEMP/pcs/<pg>_<ROBOT_ID>/<ROBOT_ID>
    python3 -m torrent.measure_topo <piece.db3 | dir> [...]     # explicit files or directories
"""
import argparse
import os
import sqlite3

import msgpack
import numpy as np

from posegraph.posegraph_utils import parse_chunk, vertex_to_dict, edge_to_dict
from .topo_codec import pack_topology, unpack_topology, file_id_from_name, _pack_xi

KIND_NAMES = {0: "none", 1: "q16", 2: "f32", 3: "f64"}


def content_bytes(path):
    """sqlite size (page_count * page_size); the zero padding the deconstitutor appends sits past it"""
    conn = sqlite3.connect(path)
    n, size = conn.execute("PRAGMA page_count").fetchone()[0], conn.execute("PRAGMA page_size").fetchone()[0]
    pointmap = conn.execute("SELECT SUM(LENGTH(data)) FROM pointmap").fetchone()[0] or 0
    conn.close()
    return n * size, pointmap


def measure(path):
    vertices, edges = parse_chunk(path)
    file_id = file_id_from_name(os.path.basename(path))
    blob = pack_topology(vertices, edges, file_id)
    legacy = (len(msgpack.packb([vertex_to_dict(v) for v in vertices], use_bin_type=True))
              + len(msgpack.packb([edge_to_dict(e) for e in edges], use_bin_type=True)))

    # round trip: the quantized xi only fills the skeleton until the real piece arrives
    _, dec_edges = unpack_topology(blob, file_id)
    err_t = err_r = 0.0
    for e, d in zip(edges, dec_edges):
        if e.xi is not None and d["xi"] is not None:
            diff = np.abs(np.asarray(d["xi"]) - np.asarray(e.xi).ravel())
            err_t, err_r = max(err_t, diff[:3].max()), max(err_r, diff[3:].max())
    kinds = [KIND_NAMES[_pack_xi(e.xi, "q16", bytearray())] for e in edges]

    sqlite_bytes, pointmap_bytes = content_bytes(path)
    return {
        "file": os.path.basename(path), "n_v": len(vertices), "n_e": len(edges),
        "topo": len(blob), "legacy": legacy, "kinds": kinds,
        "file_bytes": os.path.getsize(path), "sqlite": sqlite_bytes, "pointmap": pointmap_bytes,
        "err_t": err_t, "err_r": err_r,
    }


def collect(paths):
    files = []
    for p in paths:
        if os.path.isdir(p):
            files += sorted(os.path.join(p, f) for f in os.listdir(p) if f.endswith(".db3"))
        else:
            files.append(p)
    return files


def main():
    parser = argparse.ArgumentParser(description="Measure x-topo annotation size per piece")
    parser.add_argument("paths", nargs="*", help="piece .db3 files or directories of pieces")
    parser.add_argument("-p", "--posegraph", help="posegraph name; reads this robot's own pieces")
    parser.add_argument("-v", "--verbose", action="store_true", help="print one row per piece")
    args = parser.parse_args()

    paths = list(args.paths)
    if args.posegraph:
        rid = os.getenv("ROBOT_ID")
        paths.append(f"{os.getenv('VTRTEMP')}/pcs/{args.posegraph}_{rid}/{rid}")
    files = collect(paths)
    if not files:
        parser.error("no pieces found")

    rows = []
    for f in files:
        try:
            rows.append(measure(f))
        except Exception as e:
            print(f"skip {os.path.basename(f)}: {e}")
    if not rows:
        return

    if args.verbose:
        print(f"{'file':<22}{'V':>3}{'E':>3}{'x-topo':>8}{'legacy':>8}{'sqlite':>10}{'pointmap':>10}{'file':>10}  xi kinds")
        for r in rows:
            print(f"{r['file']:<22}{r['n_v']:>3}{r['n_e']:>3}{r['topo']:>8}{r['legacy']:>8}"
                  f"{r['sqlite']:>10}{r['pointmap']:>10}{r['file_bytes']:>10}  {','.join(r['kinds'])}")

    def col(k):
        return np.array([r[k] for r in rows], dtype=float)

    def stats(name, k, unit="B"):
        x = col(k)
        print(f"  {name:<28} mean {x.mean():>12,.1f}  median {np.median(x):>12,.1f}  max {x.max():>12,.0f} {unit}")

    kinds = [k for r in rows for k in r["kinds"]]
    print(f"\n{len(rows)} pieces, {int(col('n_v').sum())} vertices, {int(col('n_e').sum())} edges")
    stats("vertices / piece", "n_v", "")
    stats("edges / piece", "n_e", "")
    stats("x-topo annotation", "topo")
    stats("legacy msgpack annotation", "legacy")
    stats("sqlite content (pre-pad)", "sqlite")
    stats("compressed pointmap", "pointmap")
    stats("piece file on disk", "file_bytes")
    print(f"  xi encodings: " + ", ".join(f"{n} {kinds.count(n)}" for n in KIND_NAMES.values() if kinds.count(n)))
    print(f"  max round-trip error: {col('err_t').max():.2e} m, {col('err_r').max():.2e} rad")
    print(f"  x-topo / piece file:   {col('topo').sum() / col('file_bytes').sum():.2e}")
    print(f"  x-topo / legacy:       {col('topo').sum() / col('legacy').sum():.2f}")
    print(f"  zero padding on disk:  {1 - col('sqlite').sum() / col('file_bytes').sum():.1%} of piece bytes")


if __name__ == "__main__":
    main()
