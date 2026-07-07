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
from torrent_utils import *

import pdb

# zenohd --cfg 'scouting/multicast/enabled:false'

# DEVICE CONFIGS
ROBOT_IPS = {
    'mr_green':'192.168.2.42',
    'prof_plum':'192.168.3.42',
    'col_mustard':'192.168.4.42',
    'mrs_peacock':'192.168.5.42' 
    }

# Anthonys laptop
DOCKER_IPS = {
    'torrent':'172.18.0.2',
    'torrent1':'172.18.0.3'
}

class MutableSeeder:
    """
    Monitor the /pcs directory for a specific robot and seed a mutable torrent session
    This entails:
    1) update Zenoh discovery messages and broadcast
    2) seed the immutable snapshots
    """
    def __init__(self, params : dict, robot_id : int, state : dict, poll_hz : float):
        # robot params
        self.container = params['container']
        self.my_ip, z_cfg = self.unpack_device(params)
        self.router = params['router']

        # data params
        self.posegraph = params['posegraph']
        self.robot_id = robot_id
        self.state = state

        # file system
        self.input_path = f"{os.getenv('VTRTEMP')}/pcs/{self.posegraph}/{self.robot_id}"
        self.metadata_path = "scripts/torrent/metadata"
        os.makedirs(self.input_path, exist_ok=True)
        os.makedirs(self.metadata_path, exist_ok=True)
    
        # libtorrent
        self.t_ses = lt.session({
            "listen_interfaces": f"{self.my_ip}:6881,[::]:6881",
            "enable_dht": False,
            "alert_mask": (
                lt.alert.category_t.all_categories
            ),
        })

        # zenoh
        z_cfg.insert_json5("mode", '"client"')
        z_cfg.insert_json5("listen/endpoints", "[]")
        self.z_ses = zenoh.open(z_cfg)

        # mutable update
        salt = "submaps"
        sk = SigningKey(bytes.fromhex(self.state["sk"]))

        self.mi = { 
        'pubkey' : sk.verify_key.encode(),
        'salt' : salt,
        'seq' : self.state['seq'][salt],
        'infohash' : -1,
        'my_ip' : self.my_ip
        }

        # etc
        self.poll_hz = poll_hz
        self.start_flag = False

    def unpack_device(self, params : dict):
        """
        Load IPs, Zenoh config, robot names according to a config
        """
        d = params['device']

        if d == 'docker':
            my_ip = DOCKER_IPS[params['container']]
            cfg = zenoh.Config()
            tcp = '["tcp/'+ params['router'] + ':7447"]'
            cfg.insert_json5("connect/endpoints", tcp)
        elif d == 'hunter':
            my_ip = ROBOT_IPS[params['container']]
            cfg = zenoh.Config.from_file(f"../warthog/hunter2_zenoh.json5")    
        else:
            print('bad params/device')

        print(f"[unpack] my_ip {my_ip}")
        return my_ip, cfg

    def run(self):
        """
        run the main loop
        """
        print(f"[seeder] polling at {self.poll_hz} Hz (Ctrl-C to stop)")
        try: 
            while True:
                self._poll()
                time.sleep(1.0 / self.poll_hz)
        except KeyboardInterrupt:
            print("\n[seeder] stopped.")
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
            handle = self.t_ses.add_torrent({"ti" : ti, "save_path" : os.path.dirname(self.input_path)})
            print(f"[snapshot] created with hash {ti}")

            # update mutable item
            self.mi['infohash'] = infohash.to_bytes()
            self.mi['seq']+=1
            print(f"[zenoh] adding mutable item: {self.mi['infohash']}") #\n{mutable_to_string(mutable_item)}")
                
        if self.start_flag:
            # pub gossip over Zenoh
            print("[zenoh]: pub mutable item")
            payload = msgpack.packb(self.mi, use_bin_type=True)
            self.z_ses.put(f"mutable_items/{self.robot_id}", payload)      
            handle = self.t_ses.get_torrents()[0]
            status = handle.status()
            
            print(f"\tProgress: {status.progress*100:.1f}% | Peers: {status.num_peers} | Down: {status.download_rate/1000:.1f} KB/s")

    def create_snapshot(self):
        """
        Generate new .torrent for a directory
        """
        print(f"[snapshot] input path : {self.input_path}")
        fs = lt.file_storage()
        lt.add_files(fs, self.input_path, sqlite_file_filter) # filter removes -journal, -wal extensions

        t = lt.create_torrent(fs)
        
        PIECE_SIZE = 2 * 1024 * 1024 # padding
        t.piece_size(PIECE_SIZE)

        lt.set_piece_hashes(t, os.path.dirname(self.input_path))
        torrent_dict = t.generate()
        wrote_idx = False
        # annotate each entry in the dictionary
        for i, file_entry in enumerate(torrent_dict[b"info"][b"files"]):
            # if b"attr" in file_entry and b"p" in file_entry[b"attr"]: # skip padding files
            #     continue
            filename = file_entry[b"path"][-1].decode()
            if wrote_idx == False:
                idx = get_map_info(f"{self.input_path}/{filename}")
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

        out_file = os.path.join(self.metadata_path, f"{self.posegraph}.torrent")
        ti = lt.torrent_info(torrent_dict)

        with open(out_file, "wb") as f:
            f.write(lt.bencode(torrent_dict))

        return ti

# ======================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mutable Seeder (LibTorrent + Zenoh)")
    parser.add_argument('-s', '--seeder_params', type=str, default = 'seeder_params.json')
    parser.add_argument('-r', '--robot_id', type=int, default = 0)
    parser.add_argument('-q', '--state_file', type=str, default = 'mutable_state.json')
    parser.add_argument('--poll_hz', type=float, default = 0.25)
    args = parser.parse_args()

    with open(f'torrent/{args.seeder_params}', "r") as f:
                params = json.load(f)

    with open(f'torrent/{args.state_file}', "r") as f:
                state = json.load(f)

    mutable_seeder = MutableSeeder(
        params=params,
        robot_id=args.robot_id,
        state=state,
        poll_hz=args.poll_hz
    )
    mutable_seeder.run()