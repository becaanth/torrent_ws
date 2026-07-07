#!/usr/bin/env python3
import libtorrent as lt
import time
import zenoh
import msgpack
from queue import Queue
import os
import argparse

from torrent_utils import *

import pdb

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

class MutablePeer:
    """
    Listen to Zenoh gossip, join a torrent session
    """
    def __init__(self, params : dict, state : dict):
        # robot params
        self.container = params['container']
        self.peers = []
        self.my_ip, z_cfg = self.unpack_device(params)
        self.router = params['router']

        # data params
        self.posegraph = params['posegraph']
        self.state = state

        # file system
        self.input_path = f"{os.getenv('VTRTEMP')}/torrent_pcs/{self.posegraph}"
        os.makedirs(self.input_path, exist_ok=True)

        # libtorrent
        self.t_ses = lt.session({
            "listen_interfaces": f"{self.my_ip}:6881,[::]:6881",
            'enable_dht': False,
            'alert_mask': (
                lt.alert.category_t.all_categories
            )
        })

        # zenoh
        self.message_queue = Queue()
        z_cfg.insert_json5("mode", '"client"')
        z_cfg.insert_json5("listen/endpoints", "[]")
        self.z_ses = zenoh.open(z_cfg)
        self.sub = self.z_ses.declare_subscriber("mutable_items/**", self.on_sample)
        
        # mutable updates
        salt = "submaps" 
        self.mi = {
            'pubkey' : b'1',
            'salt' : salt,
            'seq' : -1,
            'infohash' : -1,
            'my_ip' : self.my_ip
        }
        self.seq = self.mi['seq'] # sequence number

        # etc

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
            for key in DOCKER_IPS.keys():
                if key != params['container']:
                    self.peers.append((DOCKER_IPS[key], 6881))
        elif d == 'hunter':
            my_ip = ROBOT_IPS[params['container']]
            cfg = zenoh.Config.from_file(f"../warthog/hunter2_zenoh.json5")    
            for key in ROBOT_IPS.keys():
                if key != params['robot_id']:
                    self.peers.append((ROBOT_IPS[key], 6881))
        else:
            print('bad params/device')

        print(f"[unpack] my_ip {my_ip}")
        return my_ip, cfg

    def run(self):
        """
        run the main loop
        """
        print(f"[peer] running the main loop (Ctrl-C to stop)")
        try:
            while True:
                self._flush_queue()
        except KeyboardInterrupt:
            print("\n[peer] stopped")
        finally:
            self.z_ses.close()

    def _flush_queue(self):
        """
        flush self.message_queue
        """
        # Check for new messages (non-blocking)
        if not self.message_queue.empty():
            sample = self.message_queue.get()
            
            self.mi = on_mutable_item(sample)
            print(f"[zenoh]: new mutable item: \n{mutable_to_string(self.mi)}")
            
            # i.e. if new infohash
            if self.mi['seq'] > self.seq: 
                print(f"[torrent]: adding \n\t infohash : {self.mi['infohash']} \n\t peers : {self.peers}")
                for handle in self.t_ses.get_torrents():
                    self.t_ses.remove_torrent(handle)

                h = self.t_ses.add_torrent({
                    'info_hash': self.mi['infohash'],
                    'save_path': self.input_path,
                })
                print(f"attempting connect_peer to {self.peers}")
                for ip, p in self.peers:
                    h.connect_peer((ip, p))

                s = h.status()
                print(f"\tProgress: {s.progress*100:.1f}% | Peers: {s.num_peers} | Down: {s.download_rate/1000:.1f} KB/s")
                self.seq = self.mi['seq'] # dont duplicate torrent handles

        # Monitor existing torrents
        for handle in self.t_ses.get_torrents():
            s = handle.status()
            print(f"[{handle.info_hash()}] Progress: {s.progress*100:.1f}%")
            # for a in ses.pop_alerts():
            #     print(f"[alert] {a}")
                    
        time.sleep(2)

    def on_sample(self, sample):
        print('Received Zenoh message')
        self.message_queue.put(sample)


# ======================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mutable Peer (LibTorrent + Zenoh)")
    parser.add_argument('-s', '--peer_params', type=str, default = 'peer_params.json')
    parser.add_argument('-q', '--state_file', type=str, default = 'mutable_state.json')
    args = parser.parse_args()

    with open(f'torrent/{args.peer_params}', "r") as f:
                params = json.load(f)

    with open(f'torrent/{args.state_file}', "r") as f:
                state = json.load(f)

    mutable_peer = MutablePeer(
        params=params,
        state=state
    )
    mutable_peer.run()