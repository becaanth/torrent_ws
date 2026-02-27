# seeder.py
import libtorrent as lt
from nacl.signing import SigningKey
import time
import zenoh
import os
import json
import msgpack
import argparse
import pdb

try:
    if os.path.exists('scripts/torrent/seeder_params.json'):
        with open('scripts/torrent/seeder_params.json', "r") as f:
            params =  json.load(f)
except:
    print("can't open params, navigate to torrent_ws")

if params['device'] == 'docker':
    TORRENT_WS = "/home/asrl/ASRL/vtr3/torrent_ws"
elif params['device'] == 'hunter':
    TORRENT_WS = "/home/indro/ASRL/vtr3/torrent_ws"
else:
    print('bad params')


PATH = f"{TORRENT_WS}/deconstructed/0/{params['posegraph']}"
STATE_FILE = f"{TORRENT_WS}/scripts/torrent/mutable_state.json"

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

MY_IP = DOCKER_IPS[params['robot_id']]

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

# -------------------------
# Immutable snapshot
# -------------------------
def create_snapshot(path):
    """
    Generate new .torrent for a directory
    """
    fs = lt.file_storage()
    lt.add_files(fs, path)

    t = lt.create_torrent(fs)
    
    PIECE_SIZE = 2 * 1024 * 1024 # padding
    t.piece_size(PIECE_SIZE)

    lt.set_piece_hashes(t, os.path.dirname(path))

    ti = lt.torrent_info(t.generate())
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
        return True
    
    return False

def mutable_to_string(mutable_item):
    return f"key: {mutable_item['pubkey']}\n salt: {mutable_item['salt']} \n seq: {mutable_item['seq']} \n infohash: {mutable_item['infohash']} \n my IP: {mutable_item['my_ip']}"


# ======================================================

if __name__ == "__main__":
    print(f"my IP: {MY_IP}")

    path = PATH #input("Directory to seed: ").strip()
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

    # initiate torrent session
    ses = lt.session({
        "listen_interfaces": f"{MY_IP}:6881,[::]:6881",
        "enable_dht": False,
        "alert_mask": (
            lt.alert.category_t.all_categories
        ),
    })
    print(ses.listen_port())

    # cfg = zenoh.Config.from_file("hunter2_zenoh.json5")

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

    # callback loop
    start_flag = False
    os.makedirs(path, exist_ok=True)
    try: 
        while True:
            if has_new_file(path):
                print("New file added!")
                # Create snapshot
                ti = create_snapshot(path)
                infohash = ti.info_hash()
                mutable_item['infohash'] = infohash.to_bytes()
                mutable_item['seq']+=1
                print(f"New mutable item: \n{mutable_to_string(mutable_item)}")
                h = ses.add_torrent({
                    "ti" : ti,
                    "save_path" : os.path.dirname(path)
                })
                payload = msgpack.packb(mutable_item, use_bin_type=True)
                start_flag = True
                session.put(f"mutable_items/{params['robot_id']}", payload)
                    
            if start_flag:
                s = h.status()
                print(f"Progress: {s.progress*100:.1f}% | Peers: {s.num_peers} | Down: {s.download_rate/1000:.1f} KB/s")
        
            # alerts
            # for a in ses.pop_alerts():
            #     print(a)
            time.sleep(5)

    except KeyboardInterrupt:
        print("\nExiting...")