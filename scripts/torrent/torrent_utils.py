import time
import os
import json
import msgpack
import pprint
import libtorrent as lt
from posegraph.posegraph_utils import *

# -------------------------
# Persistence
# -------------------------
def load_state(state_file):
    if os.path.exists(state_file):
        with open(state_file, "r") as f:
            return json.load(f)
    print(f"[load_state] couldn't load state")
    return None

def save_state(state, state_file):
    with open(state_file, "w") as f:
        json.dump(state, f)


def drain_alerts(ses, timeout=10):
    print('drain alerts')
    end = time.time() + timeout
    while time.time() < end:
        for a in ses.pop_alerts():
            print(a)
        time.sleep(0.2)

def has_new_file(directory, last_count=[0]):
    """
    Check if file count has increased. True if new file added
    """
    current_count = len(os.listdir(directory))
    if current_count > last_count[0]:
        last_count[0] = current_count
        tf = True
    else:
        tf = False

    print(f'Is there a new file? {tf}')    
    return tf

def sqlite_file_filter(file_path: str) -> bool:
    """
    Returns True if .db3, else False
    """
    # libtorrent usually passes the full or relative path as a string
    filename = os.path.basename(file_path)
    
    # Filter out SQLite write-ahead logs and rollback journals
    if filename.endswith('.db3-wal') or filename.endswith('.db3-journal'):
        print(f"Skipping temporary SQLite file: {filename}")
        return False
        
    return True

def mutable_to_string(mutable_item):
    return f"\tkey: {mutable_item['pubkey']}\n \tsalt: {mutable_item['salt']} \n \tseq: {mutable_item['seq']} \n \tinfohash: {mutable_item['infohash']} \n \tmy IP: {mutable_item['my_ip']}"

# inspection
def inspect_torrent(path):
    raw = lt.bdecode(open(path, "rb").read())
    info = raw[b"info"]

    # get MapInfo
    idx = msgpack.unpackb(info[b'files'][0][b'x-idx'], raw=False)

    pieces : list[Piece] = []
    for file_entry in info.get(b"files", []):
        rel_path = "/".join(p.decode() for p in file_entry[b"path"])
        size = file_entry[b"length"]
        extras = {
            k.decode(): v
            for k, v in file_entry.items()
            if k not in {b"length", b"path", b"attr", b"path.utf-8"}
        }
        print(f"  {rel_path}  ({size} bytes)")
        if extras:
            raw_vertices = msgpack.unpackb(file_entry[b"x-vertices"], raw=False)
            raw_edges    = msgpack.unpackb(file_entry[b"x-edges"],    raw=False)
            # pprint.pprint(raw_vertices, indent=4)
            # pprint.pprint(raw_edges, indent=8)

            # package into classes
            vertices, edges = [],[]
            for raw_vertex in raw_vertices:
                vertices.append(Vertex(vertex_id=raw_vertex['id']))
            for raw_edge in raw_edges:
                edges.append(Edge(
                    from_id=raw_edge['from'],
                    to_id=raw_edge['to'],
                    mode=raw_edge['mode'],
                    type=raw_edge['type'],
                    xi=raw_edge['xi'],
                ))
            pieces.append(Piece(top_vertices=vertices, top_edges=edges, metadata_written=False))

    return pieces, idx['idx']

def on_mutable_item(sample):
    # Update mutable item
    zenoh_item = msgpack.unpackb(bytes(sample.payload), raw=False)
    mutable_item = {
        'pubkey' : zenoh_item.get('pubkey'),
        'salt' : 'submaps',
        'seq' : zenoh_item.get('seq'),
        'infohash' : zenoh_item.get('infohash'),
        'my_ip' : zenoh_item.get('my_ip')
    }

    return mutable_item