# peer.py
import libtorrent as lt
import time
import os

def download_from_magnet(magnet_uri, download_path):
    ses = lt.session()
    ses.listen_on(6881, 6891)
    ses.start_dht()
    ses.start_lsd()

    params = {
        'save_path': download_path,
        'storage_mode': lt.storage_mode_t.storage_mode_sparse,
    }

    print("[+] Adding magnet link...")
    h = lt.add_magnet_uri(ses, magnet_uri, params)

    print("[+] Waiting for metadata...")
    while not h.has_metadata():
        time.sleep(1)

    print(f"[+] Metadata acquired: {h.name()}")
    print("[+] Downloading...")

    while not h.is_seed():
        s = h.status()
        print(f"[DOWNLOADING] {s.progress * 100:.2f}% - "
              f"Peers: {s.num_peers} - "
              f"Download: {s.download_rate / 1000:.2f} kB/s")
        time.sleep(2)
        # Print list of connected peers
        peer_info = h.get_peer_info()
        print(f"    ↪ Connected peers ({len(peer_info)}):")
        for peer in peer_info:
            print(f"        - {peer.ip} ({peer.client})")

            print("[+] Download complete.")
            print(f"[+] File saved to: {download_path}/{h.name()}")

if __name__ == "__main__":
    magnet_uri = input("Paste magnet URI: ").strip()
    download_path = input("Enter folder to save the file: ").strip()

    if not os.path.exists(download_path):
        os.makedirs(download_path)

    download_from_magnet(magnet_uri, download_path)
