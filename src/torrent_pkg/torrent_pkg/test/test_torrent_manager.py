import os
import tempfile
import time
import libtorrent as lt

from torrent_pkg.torrent.torrent_manager import TorrentManager  # your minimal class

# ------------------------------
# Step 1: Create temp directories
# ------------------------------
peer0_dir = tempfile.mkdtemp()  # Seeder
peer1_dir = tempfile.mkdtemp()  # Leecher 1
peer2_dir = tempfile.mkdtemp()  # Leecher 2

print("Seeder dir:", peer0_dir)
print("Leecher1 dir:", peer1_dir)
print("Leecher2 dir:", peer2_dir)

# ------------------------------
# Step 2: Create two dummy pieces
# ------------------------------
piece_files = []
for piece_id in range(2):
    file_path = os.path.join(peer0_dir, f"{piece_id}.db3")
    with open(file_path, "w") as f:
        f.write(f"dummy data for piece {piece_id}")
    piece_files.append(file_path)

# ------------------------------
# Step 3: Initialize TorrentManagers
# ------------------------------
tm_seed = TorrentManager(peer0_dir)
tm_leech1 = TorrentManager(peer1_dir)
tm_leech2 = TorrentManager(peer2_dir)

# ------------------------------
# Step 4: Seed pieces and save torrent files
# ------------------------------
torrent_paths = []
for piece_id, file_path in enumerate(piece_files):
    # Seed piece
    tm_seed.seed_piece(piece_id)

    # Build torrent file
    fs = lt.file_storage()
    lt.add_files(fs, file_path)
    t = lt.create_torrent(fs)
    lt.set_piece_hashes(t, os.path.dirname(peer0_dir))
    torrent = t.generate()

    torrent_path = os.path.join(peer0_dir, f"{piece_id}.torrent")
    with open(torrent_path, "wb") as f:
        f.write(lt.bencode(torrent))
    torrent_paths.append(torrent_path)

print("Torrent files created:", torrent_paths)

# ------------------------------
# Step 5: Leecher downloads
# ------------------------------
for piece_id, torrent_path in enumerate(torrent_paths):
    ti = lt.torrent_info(torrent_path)

    handle1 = tm_leech1._session.add_torrent({
        'ti': ti,
        'save_path': peer1_dir
    })
    tm_leech1._handles[piece_id] = handle1
    tm_leech1._state[piece_id] = "DOWNLOADING"

    handle2 = tm_leech2._session.add_torrent({
        'ti': ti,
        'save_path': peer2_dir
    })
    tm_leech2._handles[piece_id] = handle2
    tm_leech2._state[piece_id] = "DOWNLOADING"

# ------------------------------
# Step 6: Poll loop and live visualization
# ------------------------------
timeout = 15
start = time.time()

print("\nPolling torrents... progress table updates:\n")
while True:
    tm_seed.poll()
    tm_leech1.poll()
    tm_leech2.poll()

    print("Seeder status:")
    tm_seed.visualize()
    print("Leecher1 status:")
    tm_leech1.visualize()
    print("Leecher2 status:")
    tm_leech2.visualize()
    print("\n" + "-"*40 + "\n")

    all_complete = True
    for tm in [tm_leech1, tm_leech2]:
        if any(tm._state[pid] != "SEEDING" for pid in tm._handles):
            all_complete = False
            break

    if all_complete:
        print("All pieces downloaded by all leechers!")
        break

    if time.time() - start > timeout:
        print("Timeout: some pieces did not complete in time")
        break

    time.sleep(1)
