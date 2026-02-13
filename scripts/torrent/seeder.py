# seeder.py
import libtorrent as lt
import time
import os
import socket
import pdb

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('10.255.255.255', 1))
        return s.getsockname()[0]
    except:
        return '127.0.0.1'
    finally:
        s.close()

def create_and_seed_torrent(file_path):
    ses = lt.session()
    ses.listen_on(6881, 6891)
    ses.start_dht()
    ses.start_lsd()

    fs = lt.file_storage()
    lt.add_files(fs, file_path)

    t = lt.create_torrent(fs)
    lt.set_piece_hashes(t, os.path.dirname(file_path))
    torrent = t.generate()

    pdb.set_trace()

    torrent_path = file_path + '.torrent'
    with open(torrent_path, "wb") as f:
        f.write(lt.bencode(torrent))

    print(f"[+] Torrent file created: {torrent_path}")

    ti = lt.torrent_info(torrent)
    params = {
        'save_path': os.path.dirname(file_path),
        'ti': ti
    }

    h = ses.add_torrent(params)
    magnet_uri = lt.make_magnet_uri(ti)

    print(f"[+] Seeding started for: {h.name()}")
    print(f"[+] Magnet URI:\n{magnet_uri}")
    print(f"[+] Share this URI with peers on the same network ({get_local_ip()})")

    try:
        while True:
            s = h.status()
            print(f"[SEEDING] Peers: {s.num_peers} - Upload: {s.upload_rate / 1000:.2f} kB/s")
            time.sleep(5)
            # Print list of connected peers
            peer_info = h.get_peer_info()
            print(f"    ↪ Connected peers ({len(peer_info)}):")
            for peer in peer_info:
                print(f"        - {peer.ip} ({peer.client})")
    except KeyboardInterrupt:
        print("\n[-] Seeder stopped.")

if __name__ == "__main__":
    file_path = input("Enter the path to the file: ").strip()
    if not os.path.exists(file_path):
        print("[-] File not found.")
        exit(1)
    create_and_seed_torrent(file_path)
