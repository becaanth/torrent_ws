import time
import os
import json
import threading
import difflib
import msgpack
import libtorrent as lt
import zenoh
from posegraph.posegraph_utils import *
from .topo_codec import unpack_topology, file_id_from_name

logger = logging.getLogger(__name__)

# DEVICE CONFIGS
ROBOT_IPS = {
    'base' : '10.223.0.10', # this is Anthonys laptop; ip route get 1.1.1.1 | awk '{print $7}'
    'mr_green':'192.168.2.42',
    'prof_plum':'192.168.3.42',
    'col_mustard':'192.168.4.42',
    'mrs_peacock':'192.168.5.42' 
}

# Anthonys laptop
DOCKER_IPS = {
    'torrent15':'172.18.0.2',
    'torrent1':'172.18.0.3',
    'torrent2':'172.18.0.4',
    'torrent3':'172.18.0.4',
    'torrent4':'172.18.0.3',
    'torrent8':'172.18.0.3',
}

# ===================================================================== METADATA DIFFS
# Each snapshot's info dict is the previous one plus a few files. Instead of every peer fetching
# the whole info dict over ut_metadata at every seq, the seeder publishes
#     D[seq] = info_diff(info[seq-1], info[seq])        (D[1] is the diff from {}, i.e. everything)
# and peers patch their own copy. Only the seeder makes diffs; everyone else stores and forwards
# the exact blobs, labelled (robot, epoch, seq). A patched dict is trusted only if its infohash
# matches the gossiped one, so a bad or missing diff costs a fallback to ut_metadata, never a
# wrong torrent.
#   dict        -> {"d": {key: sub-diff for added/changed keys}, "x": [removed keys]}
#   list/bytes  -> {"h": [[p, q, new], ...]}: hunks, each replacing a[p:q] with new (p ascending,
#                  non-overlapping). Several hunks because one snapshot can add files at several
#                  places in the sorted file list (e.g. two sequences' pieces interleaved by name).
#                  b"pieces" is matched in 20-byte SHA-1 units, so its hunks line up with files.
#   anything    -> {"v": new value}
SHA1_LEN = 20

def _hunks(ka, kb):
    """(i1, i2, j1, j2) for every non-equal stretch between two sequences of hashable keys"""
    sm = difflib.SequenceMatcher(None, ka, kb, autojunk=False)   # pad entries repeat: no autojunk
    return [(i1, i2, j1, j2) for tag, i1, i2, j1, j2 in sm.get_opcodes() if tag != "equal"]

def info_diff(a, b):
    if a == b:
        return None
    if isinstance(a, dict) and isinstance(b, dict):
        return {"d": {k: info_diff(a.get(k), b[k]) for k in b if k not in a or a[k] != b[k]},
                "x": [k for k in a if k not in b]}
    if isinstance(a, list) and isinstance(b, list):
        key = lambda x: msgpack.packb(x, use_bin_type=True)
        return {"h": [[i1, i2, b[j1:j2]] for i1, i2, j1, j2 in _hunks([key(x) for x in a], [key(x) for x in b])]}
    if isinstance(a, bytes) and isinstance(b, bytes):
        u = SHA1_LEN if (len(a) % SHA1_LEN == 0 and len(b) % SHA1_LEN == 0 and min(len(a), len(b)) >= 10 * SHA1_LEN) else 0
        if u:   # concatenated piece hashes
            blocks = lambda x: [x[i:i + u] for i in range(0, len(x), u)]
            return {"h": [[i1 * u, i2 * u, b[j1 * u:j2 * u]] for i1, i2, j1, j2 in _hunks(blocks(a), blocks(b))]}
        n = min(len(a), len(b))      # short bytes: one hunk between common prefix and suffix
        p = 0
        while p < n and a[p] == b[p]:
            p += 1
        s = 0
        while s < n - p and a[-1 - s] == b[-1 - s]:
            s += 1
        return {"h": [[p, len(a) - s, b[p:len(b) - s]]]}
    return {"v": b}

def info_patch(a, d):
    """Apply info_diff(a, b) to a, returning b. Raises ValueError if d can't belong to a."""
    if d is None:
        return a
    if "v" in d:
        return d["v"]
    if "h" in d:
        if not isinstance(a, (list, bytes)):
            raise ValueError("diff does not apply to this base")
        out, end = a, len(a)
        for p, q, new in reversed(d["h"]):         # back to front: earlier offsets stay valid
            if type(new) is not type(a) or not (0 <= p <= q <= end):
                raise ValueError("diff does not apply to this base")
            out = out[:p] + new + out[q:]
            end = p
        return out
    if not isinstance(a, dict):
        raise ValueError("diff does not apply to this base")
    out = {k: v for k, v in a.items() if k not in d["x"]}
    for k, sub in d["d"].items():
        out[k] = info_patch(a.get(k), sub)
    return out

def pack_diff(d) -> bytes:
    return msgpack.packb(d, use_bin_type=True)

def unpack_diff(blob: bytes):
    # info dict keys are bytes, and a diff-range reply is keyed by int seq: allow non-str map keys
    return msgpack.unpackb(blob, raw=False, strict_map_key=False)

class DiffLog:
    """
    Every diff this agent has seen, for every robot, so it can serve them to agents that missed some.
    Keeps only the newest epoch per robot (diffs from before a seeder restart are useless).
    ~1 kB per snapshot: a 90 min run is well under 1 MB per robot.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self._log = {}      # robot -> (epoch, {seq: blob})

    def store(self, robot, epoch, seq, blob):
        if blob is None:
            return
        robot, epoch, seq = int(robot), int(epoch), int(seq)
        with self._lock:
            ep, log = self._log.get(robot, (None, None))
            if ep is None or epoch > ep:
                self._log[robot] = (epoch, {seq: bytes(blob)})
            elif epoch == ep:
                log.setdefault(seq, bytes(blob))     # same seq = same blob (only the seeder makes them)

    def get_range(self, robot, epoch, a, b) -> dict:
        """{seq: blob} for whichever seqs in [a, b] this agent holds (possibly none)."""
        with self._lock:
            ep, log = self._log.get(int(robot), (None, {}))
            if ep != int(epoch):
                return {}
            return {s: log[s] for s in range(int(a), int(b) + 1) if s in log}

    def discard(self, robot, epoch, seqs):
        """Forget diffs that failed to verify, so they aren't served to anyone else."""
        with self._lock:
            ep, log = self._log.get(int(robot), (None, {}))
            if ep == int(epoch):
                for s in seqs:
                    log.pop(int(s), None)

def load_state(state_file): # TODO
    if os.path.exists(state_file):
        with open(state_file, "r") as f:
            return json.load(f)
    logging.debug(f"[load_state] couldn't load state")
    return None

def save_state(state, state_file): # TODO
    with open(state_file, "w") as f:
        json.dump(state, f)

def drain_alerts(ses, timeout=10):
    logging.debug('drain alerts')
    end = time.time() + timeout
    while time.time() < end:
        for a in ses.pop_alerts():
            logging.debug(a)
        time.sleep(0.2)

def sqlite_file_filter(file_path: str, current_vertex_id: int) -> bool:
    return filter_in_progress_files(file_path) & filter_repeat_files(file_path, current_vertex_id)

def filter_in_progress_files(file_path: str) -> bool:
    """ Returns True if .db3, else False """
    # libtorrent usually passes the full or relative path as a string
    filename = os.path.basename(file_path)
    
    # Filter out SQLite write-ahead logs and rollback journals
    if filename.endswith('.db3-wal') or filename.endswith('.db3-journal'):
        # logging.debug(f"Skipping temporary SQLite file: {filename}")
        return False

    return True

def filter_repeat_files(file_path: str, current_vertex_id: int) -> bool:
    """ When repeating and torrenting, for experiments, we want to filter only pieces that have come before this localized submap"""
    # get filename as hex
    base_name = os.path.basename(file_path)
    hex_str, _ = os.path.splitext(base_name)
    # logging.debug(f"file_path {file_path}, hex_str {hex_str}, current_vtx {current_vertex_id}")
    try:
        hex_val = int(hex_str, 16)
    except ValueError:
        # logging.debug("ValueError")
        return False
    # logging.debug(f"hex_val {hex_val}")

    # TODO: we only want to filter by run id, but for now, with only one run id per robot, we can just filter by magnitude
    return current_vertex_id >= hex_val

def mutable_to_string(mutable_item):
    return f"robot id: {mutable_item['robot_id']}, seq: {mutable_item['seq']}, infohash: {mutable_item['infohash']}, my IP: {mutable_item['my_ip']}"

# inspection
def inspect_torrent(encoded_info):
    info = lt.bdecode(encoded_info)
    files = info.get(b"files", [])
    
    # guard MapInfo
    try:
        idx = msgpack.unpackb(info[b'files'][0][b'x-idx'], raw=False)
        if isinstance(idx, dict) and "idx" in idx:      # seeder writes {"idx": {...}}
            idx = idx["idx"]
    except Exception:
        return [], None

    pieces : list[Piece] = []    
    for file_entry in files:
        if b"x-topo" in file_entry:                      # compact format
            raw_vertices, raw_edges = unpack_topology(file_entry[b"x-topo"], file_id_from_name(file_entry[b"path"][-1]))
        elif b"x-vertices" in file_entry:                # robots still on the old seeder
            raw_vertices = msgpack.unpackb(file_entry[b"x-vertices"], raw=False)
            raw_edges = msgpack.unpackb(file_entry[b"x-edges"], raw=False)
        else:
            continue                                     # pad file, or a file the seeder couldn't annotate
        vertices = [Vertex(vertex_id=v["id"]) for v in raw_vertices]
        edges = [Edge(from_id=e["from"], to_id=e["to"], mode=e["mode"], type=e["type"], xi=e["xi"]) for e in raw_edges]
        pieces.append(Piece(top_vertices=vertices, top_edges=edges, metadata_written=False, data_ingested=False))
    return pieces, idx

def on_mutable_item(sample):
    # Update mutable item
    zenoh_item = msgpack.unpackb(bytes(sample.payload), raw=False)
    mutable_item = {
        'pubkey' : zenoh_item.get('pubkey'),
        'robot_id' : zenoh_item.get('robot_id'),
        'seq' : zenoh_item.get('seq'),
        'infohash' : zenoh_item.get('infohash'),
        'my_ip' : zenoh_item.get('my_ip')
    }

    return mutable_item

def unpack_device(params: dict, robot_id, zenoh_port):
    """
    Load IPs and build explicit Zenoh listen/connect configuration.
    """
    d = params['device']
    cfg = zenoh.Config()
    cfg.insert_json5("mode", '"peer"')
    cfg.insert_json5("scouting/gossip/enabled", "true")
    cfg.insert_json5("connect/exit_on_failure", "false")
    cfg.insert_json5("connect/timeout_ms", "-1")

    if d == 'docker':
            my_ip = DOCKER_IPS[f"torrent{robot_id}"]
            cfg.insert_json5("listen/endpoints", json.dumps([f"tcp/0.0.0.0:{zenoh_port}"]))
            
            # Connect to host base AND all other torrent peer container IPs
            peer_targets = [f"tcp/{ROBOT_IPS['base']}:{zenoh_port}"]
            for name, ip in DOCKER_IPS.items():
                if ip != my_ip:
                    peer_targets.append(f"tcp/{ip}:{zenoh_port}")
                    
            cfg.insert_json5("connect/endpoints", json.dumps(peer_targets))
    elif d == 'hunter':
        container = params.get('container', 'base')
        my_ip = ROBOT_IPS[container]
        cfg.insert_json5("listen/endpoints", json.dumps([f"tcp/0.0.0.0:{zenoh_port}"]))
        peer_targets = [
            f"tcp/{ip}:{zenoh_port}"
            for name, ip in ROBOT_IPS.items()
            if ip != my_ip
        ]
        cfg.insert_json5("connect/endpoints", json.dumps(peer_targets))
    else:
        raise ValueError(f"[unpack_device] Invalid device parameter: {d}")

    logging.debug(f"[unpack] Role: PEER | My IP: {my_ip} | Listening: tcp/0.0.0.0:{zenoh_port}")
    return my_ip, cfg
