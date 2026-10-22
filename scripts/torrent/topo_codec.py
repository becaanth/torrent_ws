"""
topo_codec.py -- compact per-file topology annotation ("x-topo") for snapshot torrents.

Replaces the per-file x-vertices / x-edges msgpack dicts (~585 B per submap) with one small
binary blob (~80 B for a typical 5-vertex chain submap). Lives in torrent/ next to torrent_utils.

    blob = pack_topology(vertices, edges, file_id)          # seeder, per file entry
    verts, edges = unpack_topology(blob, file_id)            # inspect_torrent
      -> the same dicts vertex_to_dict / edge_to_dict produce:
         {"id": int}  and  {"from", "to", "mode", "type", "xi": [6 floats] | None}

file_id is the submap id the file is named after (int("0da07c72fbfd0393", 16)); every vertex id
is stored relative to it, so a chain of consecutive vertices costs ~2 bytes total.

Layout (all integers LEB128 varints; signed ones zigzag-encoded):
    u8      version (1)
    svar    base - file_id                  (0 for files the deconstitutor names)
    var     (n_vertices << 1) | consecutive
            if not consecutive: n_vertices x svar (vid - base)
    var     n_edges
    per edge:
      svar  from - base
      svar  to   - base
      u8    type (bits 0-2) | mode (bits 3-5) | xi kind (bits 6-7)
      xi    kind 0: none | 1: 6 x int16 quantized | 2: 6 x float32 | 3: 6 x float64

xi encoding is chosen per edge:
  "q16" (default): int16 with steps XI_STEP (0.1 mm translation, 1e-5 rad rotation, assuming
        VTR's [rho; phi] order -- correct either way, only the step per slot would swap).
        Max error = step / 2. Any edge whose values do not fit (e.g. a long merge edge) or are
        not finite is stored as float64 instead, so nothing ever clips.
  "f32" : float32 (rounding error ~1e-7 relative)
  "f64" : exact
These transforms only fill the skeleton edge until the real piece arrives with the real edge.
"""
import struct

import numpy as np

VERSION = 1
XI_STEP = np.array([1e-4, 1e-4, 1e-4, 1e-5, 1e-5, 1e-5])   # m, m, m, rad, rad, rad
_NONE, _Q16, _F32, _F64 = 0, 1, 2, 3


# ---------------------------------------------------------------- varints
def _uvar(n: int, out: bytearray):
    if n < 0:
        raise ValueError("negative uvar")
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return


def _svar(n: int, out: bytearray):
    _uvar((n << 1) ^ (n >> 127) if n < 0 else n << 1, out)  # zigzag (arbitrary precision)


def _read_uvar(buf: bytes, i: int):
    n = shift = 0
    while True:
        b = buf[i]
        i += 1
        n |= (b & 0x7F) << shift
        if not b & 0x80:
            return n, i
        shift += 7


def _read_svar(buf: bytes, i: int):
    n, i = _read_uvar(buf, i)
    return (n >> 1) ^ -(n & 1), i


# ---------------------------------------------------------------- xi
def _pack_xi(xi, mode: str, out: bytearray) -> int:
    if xi is None:
        return _NONE
    x = np.asarray(xi, dtype=np.float64).ravel()
    if x.size != 6:
        raise ValueError(f"xi must have 6 values, got {x.size}")
    if mode == "q16" and np.all(np.isfinite(x)):
        q = np.round(x / XI_STEP)
        if np.all(np.abs(q) <= 32767):
            out += struct.pack("<6h", *q.astype(int))
            return _Q16
    if mode == "f32":
        out += struct.pack("<6f", *x)
        return _F32
    out += struct.pack("<6d", *x)          # exact; also the q16 out-of-range escape
    return _F64


def _unpack_xi(kind: int, buf: bytes, i: int):
    if kind == _NONE:
        return None, i
    if kind == _Q16:
        return (np.array(struct.unpack_from("<6h", buf, i)) * XI_STEP).tolist(), i + 12
    if kind == _F32:
        return list(struct.unpack_from("<6f", buf, i)), i + 24
    return list(struct.unpack_from("<6d", buf, i)), i + 48


# ---------------------------------------------------------------- public API
def pack_topology(vertices, edges, file_id: int, xi_mode: str = "q16") -> bytes:
    """
    vertices: [Vertex] or [{"id": int}]; edges: [Edge] or [edge_to_dict(...)]
    Raises ValueError if something does not fit the format (caller falls back to legacy keys).
    """
    vids = [int(v["id"] if isinstance(v, dict) else v.vertex_id) for v in vertices]
    eds = [e if isinstance(e, dict) else
           {"from": e.from_id, "to": e.to_id, "mode": e.mode, "type": e.type, "xi": e.xi}
           for e in edges]
    base = vids[0] if vids else int(file_id)

    out = bytearray([VERSION])
    _svar(base - int(file_id), out)

    consecutive = all(v == base + k for k, v in enumerate(vids))
    _uvar((len(vids) << 1) | int(consecutive), out)
    if not consecutive:
        for v in vids:
            _svar(v - base, out)

    _uvar(len(eds), out)
    for e in eds:
        mode, typ = int(e["mode"]), int(e["type"])
        if not (0 <= mode <= 7 and 0 <= typ <= 7):
            raise ValueError(f"edge mode/type out of range: mode={mode} type={typ}")
        _svar(int(e["from"]) - base, out)
        _svar(int(e["to"]) - base, out)
        flag_at = len(out)
        out.append(0)
        kind = _pack_xi(e.get("xi"), xi_mode, out)
        out[flag_at] = typ | (mode << 3) | (kind << 6)
    return bytes(out)


def unpack_topology(blob: bytes, file_id: int):
    """-> (vertex dicts, edge dicts) in vertex_to_dict / edge_to_dict form."""
    buf = bytes(blob)
    if buf[0] != VERSION:
        raise ValueError(f"unsupported x-topo version {buf[0]}")
    i = 1
    d, i = _read_svar(buf, i)
    base = int(file_id) + d

    nv, i = _read_uvar(buf, i)
    n, consecutive = nv >> 1, nv & 1
    if consecutive:
        vids = [base + k for k in range(n)]
    else:
        vids = []
        for _ in range(n):
            dv, i = _read_svar(buf, i)
            vids.append(base + dv)

    ne, i = _read_uvar(buf, i)
    edges = []
    for _ in range(ne):
        df, i = _read_svar(buf, i)
        dt, i = _read_svar(buf, i)
        flag = buf[i]
        i += 1
        xi, i = _unpack_xi(flag >> 6, buf, i)
        edges.append({"from": base + df, "to": base + dt,
                      "mode": (flag >> 3) & 7, "type": flag & 7, "xi": xi})
    if i != len(buf):
        raise ValueError(f"x-topo: {len(buf) - i} trailing bytes")
    return [{"id": v} for v in vids], edges


def file_id_from_name(name) -> int:
    """'0da07c72fbfd0393.db3' (str or bytes) -> int id the deconstitutor named it after."""
    if isinstance(name, bytes):
        name = name.decode()
    return int(name.rsplit("/", 1)[-1].split(".", 1)[0], 16)