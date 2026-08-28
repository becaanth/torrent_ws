import time
import os
import json
import msgpack
import libtorrent as lt
import zenoh
from posegraph.posegraph_utils import *

# DEVICE CONFIGS
ROBOT_IPS = {
    'base' : '10.223.0.10', # this is Anthonys laptop; ip route get 1.1.1.1 | awk '{print $7}'
    'mr_green':'192.168.2.42',
    'prof_plum':'192.168.3.42',
    # 'col_mustard':'192.168.4.42',
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

def load_state(state_file): # TODO
    if os.path.exists(state_file):
        with open(state_file, "r") as f:
            return json.load(f)
    print(f"[load_state] couldn't load state")
    return None

def save_state(state, state_file): # TODO
    with open(state_file, "w") as f:
        json.dump(state, f)

def drain_alerts(ses, timeout=10):
    print('drain alerts')
    end = time.time() + timeout
    while time.time() < end:
        for a in ses.pop_alerts():
            print(a)
        time.sleep(0.2)

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
    return f"robot id: {mutable_item['robot_id']}, seq: {mutable_item['seq']}, infohash: {mutable_item['infohash']}, my IP: {mutable_item['my_ip']}"

# inspection
def inspect_torrent(encoded_info):
    info = lt.bdecode(encoded_info)
    
    pieces : list[Piece] = []
    # guard MapIfno
    try:
        idx = msgpack.unpackb(info[b'files'][0][b'x-idx'], raw=False)
    except:
        idx = None
        return 
    
    for file_entry in info.get(b"files", []):
        extras = {
            k.decode(): v
            for k, v in file_entry.items()
            if k not in {b"length", b"path", b"attr", b"path.utf-8"}
        }
        if extras:
            raw_vertices = msgpack.unpackb(file_entry[b"x-vertices"], raw=False)
            raw_edges    = msgpack.unpackb(file_entry[b"x-edges"],    raw=False)

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
            pieces.append(Piece(top_vertices=vertices, top_edges=edges, metadata_written=False, data_ingested=False))

    return pieces, idx['idx']

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

    print(f"[unpack] Role: PEER | My IP: {my_ip} | Listening: tcp/0.0.0.0:{zenoh_port}")
    return my_ip, cfg
