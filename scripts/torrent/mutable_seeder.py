# seeder.py
import libtorrent as lt
from nacl.signing import SigningKey
import time
import zenoh
import os
import json
import msgpack
import numpy as np
import argparse

import sqlite3
import pandas as pd
from rclpy.serialization import deserialize_message, serialize_message
from rosidl_runtime_py.utilities import get_message

from dataclasses import dataclass, field
from typing import Optional

import pdb

try:
    if os.path.exists('scripts/torrent/seeder_params.json'):
        with open('scripts/torrent/seeder_params.json', "r") as f:
            params = json.load(f)
except:
    print("can't open params, navigate to torrent_ws")

ROBOT_IPS = {
    'mr_green':'192.168.2.42',
    'prof_plum':'192.168.3.42',
    'col_mustard':'192.168.4.42',
    'mrs_peacock':'192.168.5.42' 
    }

# Anthonys laptop
DOCKER_IPS = {
    'torrent':'172.18.0.3',
    'torrent1':'172.18.0.4'
}

if params['device'] == 'docker':
    TORRENT_WS = "/home/asrl/ASRL/vtr3/torrent_ws"
    MY_IP = DOCKER_IPS[params['robot_id']]
elif params['device'] == 'hunter':
    TORRENT_WS = "/home/indro/ASRL/vtr3/torrent_ws"
    MY_IP = ROBOT_IPS[params['robot_id']]
else:
    print('bad params')


TORRENT_PATH = f"{TORRENT_WS}/deconstructed/0/{params['posegraph']}"
METADATA_PATH = f"{TORRENT_WS}/scripts/torrent/metadata"
STATE_FILE = f"{TORRENT_WS}/scripts/torrent/mutable_state.json"

@dataclass
class Vertex:
    vertex_id: int

@dataclass
class Edge:
    from_id: int
    to_id: int
    xi: Optional[np.ndarray] = None
    cov: Optional[np.ndarray] = None

    def __repr__(self):
        return f"Edge({self.from_id}) -> ({self.to_id})"
    

# -------------------------
# Persistence
# -------------------------
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return None

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def parse_chunk(db_path: str) -> tuple[list[Vertex], list[Edge]]:
    """
    Parse a single deconstructed .db3 chunk.
    Returns (vertices, edges) extracted from that chunk.
    """
    conn = sqlite3.connect(db_path)

    vertices: list[Vertex] = []
    edges: list[Edge] = []

    # --- vertices -----------------------------------------------------------
    try:
        vtx_df = pd.read_sql_query(
            "SELECT topic_name, topic_type, timestamp, data FROM vertices", conn
        )
        for _, row in vtx_df.iterrows():
            try:
                msg = deserialize_message(row["data"], get_message(row["topic_type"]))
                vertices.append(Vertex(vertex_id=int(msg.id)))
            except Exception as exc:
                print(f"[parse_chunk] vertex deserialize error in {db_path}: {exc}")
    except Exception as exc:
        print(f"[parse_chunk] no vertices table in {db_path}: {exc}")

    # --- edges --------------------------------------------------------------
    try:
        edge_df = pd.read_sql_query(
            "SELECT topic_name, topic_type, timestamp, data FROM edges", conn
        )
        for _, row in edge_df.iterrows():
            try:
                msg = deserialize_message(row["data"], get_message(row["topic_type"]))
                # Only teach-mode edges (mode == 1)
                if msg.mode.mode != 1:
                    continue
                xi = np.array(msg.t_to_from.xi)
                cov = np.array(msg.t_to_from.cov).reshape(6, 6)
                edges.append(Edge(from_id=int(msg.from_id), to_id=int(msg.to_id), xi=xi, cov=cov))
            except Exception as exc:
                print(f"[parse_chunk] edge deserialize error in {db_path}: {exc}")
    except Exception as exc:
        print(f"[parse_chunk] no edges table in {db_path}: {exc}")

    conn.close()
    return vertices, edges

def vertex_to_dict(v: Vertex) -> dict:
    return {"id": v.vertex_id}

def edge_to_dict(e: Edge) -> dict:
    return {
        "from": e.from_id,
        "to":   e.to_id,
        "xi":   e.xi.tolist()  if e.xi  is not None else None,
        "cov":  e.cov.tolist() if e.cov is not None else None,
    }

# -------------------------
# Immutable snapshot
# -------------------------
def create_snapshot(input_path, output_path):
    """
    Generate new .torrent for a directory
    """
    fs = lt.file_storage()
    lt.add_files(fs, input_path)

    t = lt.create_torrent(fs)
    
    PIECE_SIZE = 2 * 1024 * 1024 # padding
    t.piece_size(PIECE_SIZE)

    lt.set_piece_hashes(t, os.path.dirname(input_path))

    torrent_dict = t.generate()

    # annotate each entry in the dictionary
    for i, file_entry in enumerate(torrent_dict[b"info"][b"files"]):
        # if b"attr" in file_entry and b"p" in file_entry[b"attr"]: # skip padding files
        #     continue
        filename = file_entry[b"path"][-1].decode()
        print(f"{input_path}/{filename}")
        vertices, edges = parse_chunk(f"{input_path}/{filename}")
        file_entry[b"x-vertices"] = msgpack.packb(
            [vertex_to_dict(v) for v in vertices], use_bin_type=True
        )
        file_entry[b"x-edges"] = msgpack.packb(
            [edge_to_dict(e) for e in edges], use_bin_type=True
        )

    out_file = os.path.join(output_path, "metadata.torrent")
    ti = lt.torrent_info(torrent_dict)

    with open(out_file, "wb") as f:
        f.write(lt.bencode(torrent_dict))

    return ti

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

def mutable_to_string(mutable_item):
    return f"\tkey: {mutable_item['pubkey']}\n \tsalt: {mutable_item['salt']} \n \tseq: {mutable_item['seq']} \n \tinfohash: {mutable_item['infohash']} \n \tmy IP: {mutable_item['my_ip']}"


# ======================================================

if __name__ == "__main__":

    input_path = TORRENT_PATH #input("Directory to seed: ").strip()
    output_path = METADATA_PATH

    salt = "submaps" #input("Salt (dataset id): ").strip()
    state = load_state()
    sk = SigningKey(bytes.fromhex(state["sk"]))

    mutable_item = {
        'pubkey' : sk.verify_key.encode(),
        'salt' : salt,
        'seq' : state['seq'][salt],
        'infohash' : -1,
        'my_ip' : MY_IP
    }

    start=time.time()
    # initiate torrent session
    ses = lt.session({
        "listen_interfaces": f"{MY_IP}:6881,[::]:6881",
        "enable_dht": False,
        "alert_mask": (
            lt.alert.category_t.all_categories
        ),
    })
    print(f'initiating torrent session took {time.time() - start}')

    start=time.time()
    if params['device'] == 'docker':
        print(f'params set for docker')
        cfg = zenoh.Config()
        tcp = '["tcp/'+ params['router'] + ':7447"]'
        cfg.insert_json5(
            "connect/endpoints",
            tcp
        )
    elif params['device'] == 'hunter':
        print(f'params set for hunter')
        cfg = zenoh.Config.from_file(f"{TORRENT_WS}/../warthog/hunter2_zenoh.json5")    

    cfg.insert_json5("mode", '"client"')
    cfg.insert_json5("listen/endpoints", "[]")
    session = zenoh.open(cfg)
    print(f'initiating zenoh session took {time.time() - start}')

    # print
    print(f"my IP: {MY_IP}")
    print(f"listening on {ses.listen_port()}")
    print("params: ",  params)
    start = time.time()

    # callback loop
    start_flag = False
    os.makedirs(input_path, exist_ok=True)
    os.makedirs(output_path, exist_ok=True)
    try: 
        while True:
            if has_new_file(input_path) or start_flag == False:
                start_flag = True
                print("[deconstructed]: new file")
                # Create snapshot
                ti = create_snapshot(input_path, output_path)
                infohash = ti.info_hash()
                mutable_item['infohash'] = infohash.to_bytes()
                mutable_item['seq']+=1
                print(f"[torrent] adding mutable item: {mutable_item['infohash']}") #\n{mutable_to_string(mutable_item)}")
                h = ses.add_torrent({
                    "ti" : ti,
                    "save_path" : os.path.dirname(input_path)
                })
                payload = msgpack.packb(mutable_item, use_bin_type=True)
                    
            if start_flag:
                print("[zenoh]: pub mutable item")
                session.put(f"mutable_items/{params['robot_id']}", payload)            
                s = h.status()
                print(f"\tProgress: {s.progress*100:.1f}% | Peers: {s.num_peers} | Down: {s.download_rate/1000:.1f} KB/s")
                if s.progress == 0.0:
                    transfer_start = time.time()
                if s.progress == 1.0:
                    print(f"completed torrent in {time.time() - transfer_start} s")
            # alerts
            # for a in ses.pop_alerts():
            #     print(a)
            time.sleep(5)

    except KeyboardInterrupt:
        print("\nExiting...")
        print(f'time from callback until killed: {time.time() - start}')
        session.close()