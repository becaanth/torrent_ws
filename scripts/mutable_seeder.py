# seeder.py
import libtorrent as lt
from nacl.signing import SigningKey
import time
import os
import json
import pdb

STATE_FILE = "mutable_state.json"

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
    fs = lt.file_storage()
    lt.add_files(fs, path)

    t = lt.create_torrent(fs)
    lt.set_piece_hashes(t, os.path.dirname(path))

    ti = lt.torrent_info(t.generate())
    return ti


# -------------------------
# Mutable publish
# -------------------------
def publish_mutable(ses, pubkey, privkey_seed, salt:bytes, infohash):
    # libtorrent expects 64-byte private key (seed + pubkey)
    privkey_64 = privkey_seed + pubkey  # Concatenate to get 64 bytes
    
    assert len(privkey_64) == 64, f"Private key must be 64 bytes, got {len(privkey_64)}"
    assert len(pubkey) == 32, f"Public key must be 32 bytes, got {len(pubkey)}"
    
    # The value should be the raw data to store
    value = infohash.to_bytes()
    
    print(f"Private key length: {len(privkey_64)} (should be 64)", flush=True)
    print(f"Public key length: {len(pubkey)} (should be 32)", flush=True)
    print(f"Value (infohash): {infohash.to_bytes().hex()}", flush=True)
    print(f"Value length: {len(infohash.to_bytes())} (should be 20)", flush=True)
    print(f"Salt: {salt} (len={len(salt)})", flush=True)
    
    # Verify assertions
    assert len(privkey_64) == 64, f"Wrong privkey length: {len(privkey_64)}"
    assert len(pubkey) == 32, f"Wrong pubkey length: {len(pubkey)}"
    
    ses.dht_put_mutable_item(
        privkey_64,  # 64 bytes: seed + pubkey
        pubkey,      # 32 bytes
        value,       # raw infohash bytes (20 bytes)
        salt         # your salt string
    )

def drain_alerts(ses, timeout=10):
    print('drain alerts')
    end = time.time() + timeout
    while time.time() < end:
        for a in ses.pop_alerts():
            print(a)
        time.sleep(0.2)

def seed_mutable(path, salt_str):
    # initiate lt session
    ses = lt.session({
        "listen_interfaces": "172.18.0.2:6881,[::]:6881",
        "enable_dht": True,
        "dht_bootstrap_nodes": "router.bittorrent.com:6881,dht.transmissionbt.com:6881,router.utorrent.com:6881",        "alert_mask": (
            lt.alert.category_t.all_categories
        ),
    })
    ses.add_dht_node(("router.bittorrent.com", 6881))
    ses.add_dht_node(("dht.transmissionbt.com", 6881))
    ses.add_dht_node(("router.utorrent.com", 6881))
    print(ses.listen_port())

    state = load_state()

    # Key handling
    if state is None or "sk" not in state:
        sk = SigningKey.generate()
        state["sk"] = sk.encode().hex()
        state["seq"] = {}
    else:
        sk = SigningKey(bytes.fromhex(state["sk"]))

    pk_bytes = sk.verify_key.encode()
    salt = salt_str.encode()

    seq = state["seq"].get(salt_str, 0)  # Start at 0 for first publish
    state["seq"][salt_str] = seq

    # Snapshot
    ti = create_snapshot(path)
    infohash = ti.info_hash()

    h = ses.add_torrent({
        "ti" : ti,
        "save_path" : os.path.dirname(path)
    })
    h = ses.get_torrents()[0]
    h.force_dht_announce()
    print(f"Seeder is now announcing hash: {h.info_hash()}")

    print("Waiting for DHT bootstrap")
    time.sleep(5)

    publish_mutable(
        ses, 
        pk_bytes,
        sk.encode(),
        salt,
        infohash
    )
    drain_alerts(ses, 10)
    save_state(state)

    print("\n[MUTABLE TORRENT PUBLISHED]")
    print(" pubkey :", pk_bytes)
    print(" salt   :", salt)
    print(" seq    :", seq)
    print(" infohash:", infohash)

    try:
        while True:
            alerts = ses.pop_alerts()
            for a in alerts:
                print(type(a), a)
     
            time.sleep(5)
            for t in ses.get_torrents():
                status = t.status()
                print(
                    f"peers:{status.num_peers} "
                    f"seeds:{status.num_seeds} "
                    f"progress:{status.progress * 100:.1f}% "
                    f"down:{status.download_rate / 1000:.1f} kB/s"
                )    
    except KeyboardInterrupt:
        print("\n[-] stopped")

if __name__ == "__main__":
    path = "/home/asrl/ASRL/vtr3/torrent_ws/deconstructed/0/test_indoors" #input("Directory to seed: ").strip()
    salt = "submaps" #input("Salt (dataset id): ").strip()

    if not os.path.isdir(path):
        print("[-] Must be a directory")
        exit(1)

    seed_mutable(path, salt)
