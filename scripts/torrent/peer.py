#!/usr/bin/env python3
import libtorrent as lt
import time
import zenoh
import msgpack
import threading
import json
from queue import Queue
import os
import pdb

try:
    if os.path.exists('scripts/torrent/peer_params.json'):
        with open('scripts/torrent/peer_params.json', "r") as f:
            params =  json.load(f)
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

PATH = f"{TORRENT_WS}/deconstructed/1/{params['posegraph']}"
STATE_FILE = f"{TORRENT_WS}/scripts/torrent/mutable_state.json"

message_queue = Queue()

def on_sample(sample):
    print('Received Zenoh message')
    message_queue.put(sample)

def on_mutable_item(sample, mutable_item):
    # Decode payload, update mutable_item
    zenoh_item = msgpack.unpackb(bytes(sample.payload), raw=False)

    mutable_item['pubkey'] = zenoh_item.get('pubkey')
    mutable_item['infohash'] = zenoh_item.get('infohash')
    mutable_item['seq'] = zenoh_item.get('seq')
    mutable_item['ip'] = zenoh_item.get('ip')

    return mutable_item

def mutable_to_string(mutable_item):
    return f"\tkey: {mutable_item['pubkey']}\n \tsalt: {mutable_item['salt']} \n \tseq: {mutable_item['seq']} \n \tinfohash: {mutable_item['infohash']} \n \tmy IP: {mutable_item['my_ip']}"

# ======================================================

if __name__ == "__main__":
    salt = "submaps" #input("Salt (dataset id): ").strip()

    mutable_item = {
        'pubkey' : b'1',
        'salt' : salt,
        'seq' : -1,
        'infohash' : -1,
        'my_ip' : MY_IP
    }

    start=time.time()
    # Create session
    ses = lt.session({
        "listen_interfaces": f"{MY_IP}:6881,[::]:6881",
        'enable_dht': False,
        'alert_mask': (
            lt.alert.category_t.all_categories
        )
    })
    print(f'initiating torrent session took {time.time() - start}')


    peers = []
    port = 6881
    for key in ROBOT_IPS.keys():
        if key != params['robot_id']:
            peers.append((ROBOT_IPS[key], port))

    old_infohash = mutable_item['infohash']

    # Configure Zenoh
    start=time.time()
    if params['device'] == 'docker':
        cfg = zenoh.Config()
        tcp = '["tcp/'+ params['router'] + ':7447"]'
        cfg.insert_json5(
            "connect/endpoints",
            tcp
        )
    elif params['device'] == 'hunter':
        cfg = zenoh.Config.from_file(f"{TORRENT_WS}/../hunter/hunter2_zenoh.json5")    

    cfg.insert_json5("mode", '"client"')
    cfg.insert_json5("listen/endpoints", "[]")
    session = zenoh.open(cfg)
    sub = session.declare_subscriber("mutable_items/**", on_sample)
    print(f'initiating zenoh session took {time.time() - start}')

    print(f"my IP: {MY_IP}")
    print(f"listening on {ses.listen_port()}")
    print("params: ",  params)

    # Process messages in main thread
    start=time.time()
    try:
        while True:
            # Check for new messages (non-blocking)
            if not message_queue.empty():
                sample = message_queue.get()
                
                mutable_item = on_mutable_item(sample, mutable_item)
                print(f"[zenoh]: new mutable item") #: \n{mutable_to_string(mutable_item)}")
                
                # i.e. if new infohash
                if mutable_item['infohash'] != old_infohash: 
                    print(f"[torrent]: adding {mutable_item['infohash']}")
                    for handle in ses.get_torrents():
                        ses.remove_torrent(handle)
                    
                    h = ses.add_torrent({
                        'info_hash': mutable_item['infohash'],
                        'save_path': PATH,
                        'peers': peers
                    })
                    
                    s = h.status()
                    # print(f"\tProgress: {s.progress*100:.1f}% | Peers: {s.num_peers} | Down: {s.download_rate/1000:.1f} KB/s")
                    old_infohash = mutable_item['infohash'] # dont duplicate torrent handles

            # Monitor existing torrents
            for handle in ses.get_torrents():
                s = handle.status()
                print(f"[{handle.info_hash()}] Progress: {s.progress*100:.1f}%")
                if s.progress == 0.0:
                    transfer_start = time.time()
                if s.progress == 1.0:
                    print(f"completed torrent in {time.time() - transfer_start} s")
            
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nExiting...")
        print(f'time from callback until killed: {time.time() - start}')
        session.close()