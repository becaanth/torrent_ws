# seeder.py
import libtorrent as lt
from nacl.signing import SigningKey
import time
import zenoh
import os
import json
import msgpack
import argparse

from posegraph.posegraph_utils import *
from .torrent_utils import *

import pdb

# zenohd --cfg 'scouting/multicast/enabled:false'

PORT=5204

class MutableSeeder:
    """
    Monitor the /pcs directory for a specific robot and seed a mutable torrent session
    This entails:
    1) update Zenoh discovery messages and broadcast
    2) seed the immutable snapshots
    """
    def __init__(self, params : dict, posegraph : str, this_robot_id : int, robot_id : int, state : dict, z_ses, t_ses, t_lock, my_ip, mutable_item = None, poll_hz : float = 0.2, on_torrent_updated=None):
        # robot params
        self.container = params['container']
        self.my_ip = my_ip
        self.z_ses = z_ses
        print("[Seeder]: unpacked config")
        self.router = params['router']

        # data params
        self.posegraph = posegraph
        self.this_robot_id = int(this_robot_id)
        self.robot_id = int(robot_id)
        self.state = state

        # file system
        self.input_path = f"{os.getenv('VTRTEMP')}/pcs/{self.posegraph}_{self.this_robot_id}/{self.robot_id}"
        print(f"[Seeder]: input path {self.input_path}")
        self.bencoded_torrent_dict = {}
        os.makedirs(self.input_path, exist_ok=True)
    
        # libtorrent
        print("[Seeder]: init lt")
        self.t_ses = t_ses
        self.t_lock = t_lock
        self.current_handle = None

        # zenoh
        print("[Seeder]: init zenoh")

        # mutable update
        sk = SigningKey(bytes.fromhex(self.state["sk"]))

        # is this robot the authority on this session?
        if self.this_robot_id == self.robot_id:
            self.mi = { 
            'pubkey' : sk.verify_key.encode(),
            'robot_id' : robot_id,
            'seq' : self.state['seq'],
            'infohash' : -1,
            'my_ip' : self.my_ip
            }
        else:
            self.mi = mutable_item
            self.mi['my_ip'] = self.my_ip

        # etc
        self.poll_hz = poll_hz
        self.start_flag = False

        # inversion of control callback
        self.on_torrent_updated = on_torrent_updated # -> update mutable item from remote
        print("[Seeder]: init done")


    def run(self):
        """
        run the main loop
        """
        print(f"[Seeder]: polling at {self.poll_hz} Hz (Ctrl-C to stop)")
        try: 
            while True:
                print(f"[Seeder]: polling peers")
                self._poll()
                time.sleep(1.0 / self.poll_hz)
        except KeyboardInterrupt:
            print("\n[Seeder]: stopped.")
        finally:
            self.z_ses.close()

    def _poll(self):
        if len(os.listdir(self.input_path)) == 0:
            return

        if has_new_file(self.input_path) or self.start_flag == False:
            self.start_flag = True

            # create snapshot of pcs dir
            ti = self.create_snapshot()
            infohash = ti.info_hash()
            with self.t_lock:
                new_handle = self.t_ses.add_torrent({"ti" : ti, "save_path" : os.path.dirname(self.input_path)})
            if self.current_handle is not None:
                 self.current_handle.pause()
                 with self.t_lock:
                    self.t_ses.remove_torrent(self.current_handle)
            self.current_handle = new_handle
            
            print(f"[Seeder]: snapshot created with hash {ti}")

            # update mutable item if authority
            if self.this_robot_id == self.robot_id:
                self.mi['infohash'] = infohash.to_bytes()
                self.mi['seq']+=1
                print(f"[Seeder]: seeding mutable item: \n{mutable_to_string(self.mi)}")
                
        if self.start_flag:
            # pub gossip over Zenoh
            print(f"[Seeder]: pub mutable item \n{mutable_to_string(self.mi)}")
            payload = msgpack.packb(self.mi, use_bin_type=True)
            print(f"[Seeder]: putting zenoh item for {self.robot_id}")
            self.z_ses.put(f"mutable_items/{self.robot_id}", payload)     
            with self.t_lock: 
                handle = self.t_ses.get_torrents()[0]
            status = handle.status()
            
            print(f"\t[Seeder]: Progress: {status.progress*100:.1f}% | Peers: {status.num_peers} | Down: {status.download_rate/1000:.1f} KB/s")
            with self.t_lock:
                for a in self.t_ses.pop_alerts():
                    if isinstance(a, (lt.peer_connect_alert, lt.peer_disconnected_alert,
                        lt.peer_error_alert, lt.listen_failed_alert,
                        lt.listen_succeeded_alert, lt.incoming_connection_alert)):
                        print(f"[Seeder] alert: {a}")

    def create_snapshot(self):
        """
        Generate new .torrent for a directory
        """
        print(f"[Seeder]: snapshot input path : {self.input_path}")
        fs = lt.file_storage()
        lt.add_files(fs, self.input_path, sqlite_file_filter) # filter removes -journal, -wal extensions

        t = lt.create_torrent(fs)
        
        PIECE_SIZE = 2 * 1024 * 1024 # padding
        t.piece_size(PIECE_SIZE)

        lt.set_piece_hashes(t, os.path.dirname(self.input_path))
        torrent_dict = t.generate()
        wrote_idx = False
        # annotate each entry in the dictionary
        for _, file_entry in enumerate(torrent_dict[b"info"][b"files"]):
            if b"attr" in file_entry and b"p" in file_entry[b"attr"]: # skip padding files
                continue

            filename = file_entry[b"path"][-1].decode()
            target_file_path = f"{self.input_path}/{filename}"

            if not os.path.exists(target_file_path) or os.path.getsize(target_file_path) == 0:
                print(f"[Seeder]: snapshot WARNING: Skipping empty or missing file {filename}")
                continue

            if os.path.exists(target_file_path + "-journal") or os.path.exists(target_file_path + "-wal"):
                continue 

            try:
                if wrote_idx == False:
                    idx = get_map_info(target_file_path)
                    file_entry[b"x-idx"] = msgpack.packb(
                        {"idx": idx_to_dict(idx)}, use_bin_type=True
                    )
                    wrote_idx = True

                vertices, edges = parse_chunk(f"{self.input_path}/{filename}")
                file_entry[b"x-vertices"] = msgpack.packb(
                    [vertex_to_dict(v) for v in vertices], use_bin_type=True
                )
                file_entry[b"x-edges"] = msgpack.packb(
                    [edge_to_dict(e) for e in edges], use_bin_type=True
                )
            except Exception as e:
                print(f"[Seeder]: snapshot ERROR parsing chunk {filename}: {e}. Skipping annotations for this file.")
                continue
            
        ti = lt.torrent_info(torrent_dict)
        self.bencoded_torrent_dict = lt.bencode(torrent_dict)

        return ti
    
    def update_mutable_item(self, mutable_item):
        print(f"[Seeder]: updating mutable item for robot {self.robot_id}")
        if self.mi['seq'] < mutable_item['seq']:
            self.mi = mutable_item
            self.mi['my_ip'] = self.my_ip

# ======================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mutable Seeder (LibTorrent + Zenoh)")
    parser.add_argument('-s', '--seeder_params', type=str, default = 'seeder_params.json')
    parser.add_argument('-r', '--robot_id', type=int, default = 0)
    parser.add_argument('-q', '--state_file', type=str, default = 'mutable_state.json')
    parser.add_argument('--poll_hz', type=float, default = 0.25)
    args = parser.parse_args()
    this_robot_id = os.getenv("ROBOT_ID")

    with open(f'torrent/{args.seeder_params}', "r") as f:
                params = json.load(f)

    with open(f'torrent/{args.state_file}', "r") as f:
                state = json.load(f)

    mutable_seeder = MutableSeeder(
        params=params,
        this_robot_id=this_robot_id,
        robot_id=args.robot_id,
        state=state,
        poll_hz=args.poll_hz
    )
    mutable_seeder.run()