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
from posegraph.posegraph_utils import *
from torrent_utils import *

from dataclasses import dataclass, field
from typing import Optional

import pdb

# zenohd --cfg 'scouting/multicast/enabled:false'

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

# -------------------------
# Immutable snapshot
# -------------------------
def create_snapshot(input_path, output_path, posegraph):
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
    wrote_idx = False
    # annotate each entry in the dictionary
    for i, file_entry in enumerate(torrent_dict[b"info"][b"files"]):
        # if b"attr" in file_entry and b"p" in file_entry[b"attr"]: # skip padding files
        #     continue
        filename = file_entry[b"path"][-1].decode()
        print(f"{input_path}/{filename}")
        if wrote_idx == False:
            idx = get_map_info(f"{input_path}/{filename}")
            file_entry[b"x-idx"] = msgpack.packb(
                {"idx": idx_to_dict(idx)}, use_bin_type=True
            )
            wrote_idx = True

        vertices, edges = parse_chunk(f"{input_path}/{filename}")
        file_entry[b"x-vertices"] = msgpack.packb(
            [vertex_to_dict(v) for v in vertices], use_bin_type=True
        )
        file_entry[b"x-edges"] = msgpack.packb(
            [edge_to_dict(e) for e in edges], use_bin_type=True
        )

    out_file = os.path.join(output_path, f"{posegraph}.torrent")
    ti = lt.torrent_info(torrent_dict)

    with open(out_file, "wb") as f:
        f.write(lt.bencode(torrent_dict))

    return ti

# ======================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Seed mutable torrents')
    parser.add_argument('-p', '--posegraph', type=str, default=None, help="Name of posegraph") 
    parser.add_argument('-a', '--agent_num', type=int, default=0, help="")
    parser.add_argument('-d', '--device', type=str, default='docker', help="running in \'docker\' or \'hunter\'")
    parser.add_argument('-s', '--seeder', type=str, default='seeder_params.json', help="path to seeder params")
    args = parser.parse_args()

    posegraph = args.posegraph
    agent = args.agent_num
    device = args.device
    seeder_params = args.seeder

    # Networking setup
    if device == 'docker':
        torrent_ws = "/home/asrl/ASRL/vtr3/torrent_ws"
        with open(f'{torrent_ws}/scripts/torrent/{seeder_params}', "r") as f:
                params = json.load(f)
        MY_IP = DOCKER_IPS[params['robot_id']]
        cfg = zenoh.Config()
        tcp = '["tcp/'+ params['router'] + ':7447"]'
        cfg.insert_json5("connect/endpoints", tcp)
    elif device == 'hunter':
        torrent_ws = "/home/indro/ASRL/vtr3/torrent_ws"
        with open(f'{torrent_ws}/scripts/torrent/{seeder_params}', "r") as f:
                params = json.load(f)
        MY_IP = ROBOT_IPS[params['robot_id']]
        cfg = zenoh.Config.from_file(f"{torrent_ws}/../warthog/hunter2_zenoh.json5")    
    else:
        print('bad params/device')

    # File setup
    input_path = f"{os.getenv('VTRTEMP')}/pcs/{params['posegraph']}"
    output_path = f"{torrent_ws}/scripts/torrent/metadata"
    state_file = f"{torrent_ws}/scripts/torrent/mutable_state.json"

    salt = "submaps" #input("Salt (dataset id): ").strip()
    state = load_state(state_file=state_file)
    sk = SigningKey(bytes.fromhex(state["sk"]))

    # Define a mutable item
    mutable_item = {
        'pubkey' : sk.verify_key.encode(),
        'salt' : salt,
        'seq' : state['seq'][salt],
        'infohash' : -1,
        'my_ip' : MY_IP
    }

    # Initiate torrent session
    start=time.time()
    ses = lt.session({
        "listen_interfaces": f"{MY_IP}:6881,[::]:6881",
        "enable_dht": False,
        "alert_mask": (
            lt.alert.category_t.all_categories
        ),
    })
    print(f'initiating torrent session took {time.time() - start}')

    # Setup Zenoh
    cfg.insert_json5("mode", '"client"')
    cfg.insert_json5("listen/endpoints", "[]")
    session = zenoh.open(cfg)

    print(f"my IP: {MY_IP} listening on {ses.listen_port()} params: {params}")

    # callback loop
    start_flag = False
    os.makedirs(input_path, exist_ok=True)
    os.makedirs(output_path, exist_ok=True)
    try: 
        while True:
            if has_new_file(input_path) or start_flag == False:
                start_flag = True
                
                # Create snapshot
                ti = create_snapshot(input_path, output_path, posegraph=params['posegraph'])
                infohash = ti.info_hash()
                h = ses.add_torrent({"ti" : ti, "save_path" : os.path.dirname(input_path)})
                mutable_item['infohash'] = infohash.to_bytes()
                mutable_item['seq']+=1
                print(f"[torrent] adding mutable item: {mutable_item['infohash']}") #\n{mutable_to_string(mutable_item)}")
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
            time.sleep(5)

    except KeyboardInterrupt:
        print("\nExiting...")
        session.close()