import time
import libtorrent as lt
import sys
import pdb

# -----------------------------
# CONFIG (these are shared OOB)
# -----------------------------
PUBKEY = b'1\x02\x0b\xa2\xc0\x9c\xc1\xc2\xdb\xeb@\xb1\xa9\xcb\xf19\xdc\x8c\x8a\xc0\x92G\xa8\xcan\xc4\x11\x19\x07\xf8\xec\x06'
SALT   = b'submaps'
SAVE_PATH = "/home/asrl/ASRL/vtr3/torrent_ws/deconstructed/2/test_indoors"
SEEDER_IP = "172.18.0.2"
SEEDER_PORT = 6881

# -----------------------------
# Session setup
# -----------------------------
ses = lt.session({
    "listen_interfaces": "172.18.0.3:6881,[::]:6881",
    "enable_dht": True,
    "dht_bootstrap_nodes": "router.bittorrent.com:6881,dht.transmissionbt.com:6881,router.utorrent.com:6881",    "alert_mask": (
        lt.alert.category_t.all_categories
    ),
})

print("Bootstrapping DHT")
print(f"Adding seeder as DHT node: {SEEDER_IP}:{SEEDER_PORT}")
ses.add_dht_node(("router.bittorrent.com", 6881))
ses.add_dht_node(("dht.transmissionbt.com", 6881))
ses.add_dht_node(("router.utorrent.com", 6881))
ses.add_dht_node((SEEDER_IP, SEEDER_PORT))

# Give DHT time to establish connection with seeder
print("Waiting for DHT to connect to seeder...")
time.sleep(5)
    
print(f"Requesting mutable item pk: {PUBKEY.hex()}, salt: {SALT}")
ses.dht_get_mutable_item(PUBKEY, SALT)

torrent_added = False
handle = None

# -----------------------------
# Event loop
# -----------------------------
while True:
    alerts = ses.pop_alerts()

    for a in alerts:
        alert_type = type(a).__name__
        print(f"[{alert_type}] {a}")
        print(f"[ALERT] {alert_type}: {a}", flush=True)  # Force flush
        sys.stdout.flush()  # Extra flush
        
        # ---- Mutable item resolved ----
        if alert_type == 'dht_mutable_item_alert':
            print("=== ENTERING MUTABLE ITEM HANDLER ===", flush=True)
            sys.stdout.flush()
            alert = a
            key = a.key
            salt = a.salt
            seq = a.seq
            sig = a.signature
            print(f'[MESSAGE] {a.message()}')
            print(f"--- DHT Mutable Item Received ---")
            print(f"Key: {key.hex()}")  # Check if this is a dict or bytes
            print(f"Sequence: {seq}")
            print(f"Salt: {salt}")

            target_infohash = None

            # Try to get the value by calling to_string on item
            try:
                # entry objects sometimes have to_string() or to_dict()
                item_str = a.item.to_string()
                print(f"Item string: {item_str}")
                decoded = lt.bdecode(item_str)
                print(f"Decoded: {decoded}")
                
                if b'v' in decoded:
                    target_infohash = lt.sha1_hash(decoded[b'v'])
                    print(f"[SUCCESS] Resolved infohash: {target_infohash}")
            except Exception as e:
                print(f"Item access error: {e}")

            if target_infohash:
                print(f"[MUTABLE ITEM FOUND] Infohash: {target_infohash}\n")

                if not torrent_added:
                    params = {
                        "info_hash": target_infohash,
                        "save_path": SAVE_PATH,
                    }
                    handle = ses.add_torrent(params)
                    # Use the handle to force-connect to the seeder
                    handle.connect_peer((SEEDER_IP, SEEDER_PORT))
                    torrent_added = True
                    print("Torrent added, forced connection to seeder...")
            else:
                print(f"Malformed item received:")


        # Metadata received
        if isinstance(a, lt.metadata_received_alert):
            print(f"[METADATA] Received metadata for torrent!")

        # Torrent finished
        if isinstance(a, lt.torrent_finished_alert):
            print(f"[COMPLETE] Download finished!")

        # ---- Torrent status ----
        if isinstance(a, lt.state_update_alert):
            for st in a.status:
                print(
                    f"peers:{st.num_peers} "
                    f"seeds:{st.num_seeds} "
                    f"progress:{st.progress * 100:.1f}% "
                    f"down:{st.download_rate / 1000:.1f} kB/s"
                )

        # Periodic manual poke if metadata is missing
    if handle and not handle.status().has_metadata:
        handle.connect_peer((SEEDER_IP, SEEDER_PORT))

    ses.post_torrent_updates()
    time.sleep(1)
