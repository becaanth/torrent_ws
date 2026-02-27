#!/usr/bin/env python3
import libtorrent as lt
import time
import zenoh
import msgpack
import threading
import pdb
from queue import Queue

TORRENT_WS = "/home/asrl/ASRL/vtr3/torrent_ws"
POSEGRAPH = "woody_convoy"
PATH = f"{TORRENT_WS}/deconstructed/1/"
STATE_FILE = f"{TORRENT_WS}/scripts/torrent/mutable_state.json"

ROBOT_ID = 'torrent1'
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

MY_IP = DOCKER_IPS[ROBOT_ID]

message_queue = Queue()

def on_sample(sample):
    print('Received Zenoh message')
    message_queue.put(sample)

# cfg = zenoh.Config.from_file("hunter2_zenoh.json5")
cfg = zenoh.Config()
# cfg.insert_json5("listen/endpoints", "[]")
# cfg.insert_json5("mode", '"client"')

cfg = zenoh.Config()
cfg.insert_json5("mode", '"client"')
cfg.insert_json5("listen/endpoints", "[]")
cfg.insert_json5(
    "connect/endpoints",
    '["tcp/zenohd:7447"]'
)
session = zenoh.open(cfg)

sub = session.declare_subscriber("mutable_items/**", on_sample)

def on_mutable_item(sample, mutable_item):
    # Decode payload, update mutable_item
    zenoh_item = msgpack.unpackb(bytes(sample.payload), raw=False)

    mutable_item['pubkey'] = zenoh_item.get('pubkey')
    mutable_item['infohash'] = zenoh_item.get('infohash')
    mutable_item['seq'] = zenoh_item.get('seq')
    mutable_item['ip'] = zenoh_item.get('ip')

    return mutable_item

def mutable_to_string(mutable_item):
    return f"key: {mutable_item['pubkey']}\n salt: {mutable_item['salt']} \n seq: {mutable_item['seq']} \n infohash: {mutable_item['infohash']} \n my IP: {mutable_item['my_ip']}"

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

    # Create session
    ses = lt.session({
        "listen_interfaces": f"{MY_IP}:6881,[::]:6881",
        'enable_dht': False,
        'alert_mask': (
            lt.alert.category_t.all_categories
        )
    })

    peers = []
    port = 6881
    for key in ROBOT_IPS.keys():
        if key != ROBOT_ID:
            peers.append((ROBOT_IPS[key], port))

    print(f"peers: {peers}")
    old_infohash = mutable_item['infohash']
    # Process messages in main thread
    try:
        while True:
            # Check for new messages (non-blocking)
            if not message_queue.empty():
                sample = message_queue.get()
                
                mutable_item = on_mutable_item(sample, mutable_item)
                print(f"New mutable item: \n{mutable_to_string(mutable_item)}")
                
                # i.e. if new infohash
                if mutable_item['pubkey'] != old_infohash: 
                    print(f"adding torrent {mutable_item['infohash']} | {mutable_item['infohash']}")
                    for handle in ses.get_torrents():
                        ses.remove_torrent(handle)
                    
                    h = ses.add_torrent({
                        'info_hash': mutable_item['infohash'],
                        'save_path': PATH,
                        'peers': peers
                    })
                    
                    s = h.status()
                    print(f"Progress: {s.progress*100:.1f}% | Peers: {s.num_peers} | Down: {s.download_rate/1000:.1f} KB/s")
                    old_infohash = mutable_item['infohash'] # dont duplicate torrent handles

            # Monitor existing torrents
            for handle in ses.get_torrents():
                s = handle.status()
                print(f"[{handle.info_hash()}] Progress: {s.progress*100:.1f}%")
            
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nExiting...")