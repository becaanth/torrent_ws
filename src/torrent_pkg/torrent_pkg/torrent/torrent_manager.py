import libtorrent as lt
import threading
import socket
import time
import os

from torrent_pkg.gossip.gossip_interface import GossipInterface
from torrent_pkg.state.piece_state import PieceState
from torrent_pkg.scheduling.scheduler import Scheduler

class TorrentManager:
    def __init__(self, pieces_path: str):
        self._pieces_path = pieces_path

        # create LibTorrent session
        self._session = lt.session()
        self._session.listen_on(6881,6891)
        self._session.start_dht()
        self._session.start_lsd()

        self._handles = {}
        self._state = {}

        self._lock = threading.Lock()

    def seed_piece(self, piece_id: int) -> str:
        """return a magnet link for a piece"""
        with self._lock:
            if piece_id in self._handles:
                return lt.make_magnet_uri(self._handles[piece_id].torrent_file())
            
            file_path = os.path.join(self._pieces_path, f"{piece_id}.db3")
            if not os.path.exists(file_path):
                raise FileNotFoundError(file_path)
            
            fs = lt.file_storage()
            lt.add_files(fs, file_path)

            t = lt.create_torrent(fs)
            lt.set_piece_hashes(t, os.path.dirname(file_path))
            ti = lt.torrent_info(t.generate())

            handle = self._session.add_torrent({
                'ti' : ti,
                'save_path' : os.path.dirname(file_path),
            })

            self._handles[piece_id] = handle
            self._state[piece_id] = "SEEDING"

            magnet = lt.make_magnet_uri(ti)
            print(f"[seed] piece {piece_id}")
            return magnet

    def download_piece(self, piece_id: int, magnet: str):
        with self._lock:
            if piece_id in self._handles:
                return
            
            handle = lt.add_magnet_uri(
                self._session,
                magnet,
                {'save_path': self._pieces_path}
            )

            self._handles[piece_id] = handle
            self._state[piece_id] = "DOWNLOADING"

            print(f"[download] piece {piece_id}")

    def poll(self):
        completed = []

        with self._lock:
            for piece_id, h in self._handles.items():
                s = h.status()

                if s.error:
                    self._state[piece_id] = "ERROR"
                    print(f"[error] piece {piece_id}")
                    completed.append(piece_id)

                else:
                    self._state[piece_id] = (
                        "SEEDING" if s.is_seeding else "DOWNLOADING"
                    )

        return completed

    def visualize(self):
        with self._lock:
            print("piece | state       | prog | peers")
            print("----------------------------------")
            for pid, h in self._handles.items():
                s = h.status()
                print(
                    f"{pid:>5} | {self._state[pid]:<11} | "
                    f"{s.progress*100:>4.0f}% | {s.num_peers}"
                )