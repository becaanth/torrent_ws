#!/usr/bin/env python3
import libtorrent as lt
import time
import zenoh
import msgpack
import pdb

ROBOT_ID = 'prof_plum'
ROBOT_IPS = {
    'mr_green':'192.168.2.42',
    'prof_plum':'192.168.3.42',
    'col_mustard':'192.168.4.42',
    'mrs_peacock':'192.168.5.42' 
    }

MY_IP = ROBOT_IPS[ROBOT_ID]

z = zenoh.open(zenoh.Config())

def on_sample(sample, mutable_item, ses):
    # Decode payload
    zenoh_item = msgpack.unpackb(bytes(sample.payload), raw=False)

    mutable_item['pubkey'] = zenoh_item.get('pubkey')
    mutable_item['infohash'] = zenoh_item.get('infohash')
    mutable_item['seq'] = zenoh_item.get('seq')
    mutable_item['ip'] = zenoh_item.get('ip')
    print(f"New mutable item: \n{mutable_to_string(mutable_item)}")

    if mutable_item['pubkey'] != b'1':
            # Add torrent by infohash + peer IP
            print('adding torrent')
            h = ses.add_torrent({
                'info_hash': mutable_item['infohash'],
                'save_path': '/home/asrl/ASRL/vtr3/torrent_ws/deconstructed/2/woody_convoy',
                'peers': [("172.18.0.2", 6881)]
            })
    s = h.status()
    print(f"Progress: {s.progress*100:.1f}% | Peers: {s.num_peers} | Down: {s.download_rate/1000:.1f} KB/s")
    
    # return mutable_item

def mutable_to_string(mutable_item):
    return f"key: {mutable_item['pubkey']}\n salt: {mutable_item['salt']} \n seq: {mutable_item['seq']} \n infohash: {mutable_item['infohash']} \n my IP: {mutable_item['my_ip']}"

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
        'listen_interfaces': '172.18.0.3:6881,[::]:6881',
        'enable_dht': False,
        'alert_mask': (
            lt.alert.category_t.all_categories
        )
    })

    # Monitor
    while True:
        with zenoh.open(zenoh.Config()) as session:
            with session.declare_subscriber("mutable_items/**") as subscriber:
                for sample in subscriber:
                    on_sample(sample, mutable_item, ses)
        

        time.sleep(1)  