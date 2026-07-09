#!/usr/bin/env python3
import libtorrent as lt
import time
import zenoh
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
    def __init__(self, params : dict, state : dict, poll_hz : float = 0.5,
                 on_torrent_discovered=None, on_metadata_received=None):
        
        # robot params
        self.container = params['container']
        self.peers = []
        self.my_ip, z_cfg = self.unpack_device(params)
        self.router = params['router']

        # data params
        self.posegraph = params['posegraph']
        self.state = state

        # file system
        self.output_path = f"{os.getenv('VTRTEMP')}/torrent_pcs/{self.posegraph}"
        os.makedirs(self.output_path, exist_ok=True)

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
        self.mutable_items = [] # TODO: support mi from multiple sessions

        # inversion of control callbacks
        self.on_torrent_discovered = on_torrent_discovered # spawn MutableSeeder # TODO wire
        self.on_metadata_received = on_metadata_received   # pass to Reconstitutor # TODO wire

        # etc
        self.poll_hz = poll_hz

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
                time.sleep(1.0 / self.poll_hz)
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
            
            mutable_item = on_mutable_item(sample)
            print(f"[zenoh]: new mutable item: \n{mutable_to_string(mutable_item)}")

            # check if new session discovered
            existing_ids = {mi['robot_id'] for mi in self.mutable_items if 'robot_id' in mi}
            if mutable_item['robot_id'] not in existing_ids:
                self.mutable_items.append(mutable_item)
                 
            # check if heard own seeder
            if mutable_item['my_ip'] == self.my_ip:
                print(f"[flush]: skipping. don't leech own pieces")
                return
            
            # check if infohash has been updated
            saved_mi = self.mutable_items[mutable_item['robot_id']]
            if mutable_item['seq'] >= saved_mi['seq']: # what if we torrent a completed map? then these are equal
                # overwrite
                print(f"[torrent]: adding \n\t infohash : {mutable_item['infohash']} \n\t peers : {self.peers}")
                self.mutable_items[mutable_item['robot_id']] = mutable_item

                for handle in self.t_ses.get_torrents():
                    if mutable_item['seq'] > saved_mi['seq']:
                        self.t_ses.remove_torrent(handle)

                save_path =  f"{self.output_path}"
                h = self.t_ses.add_torrent({
                    'info_hash': mutable_item['infohash'],
                    'save_path': save_path
                })
                print(f'save path : {save_path}')
                print(f"attempting connect_peer to {self.peers}")
                for ip, p in self.peers:
                    h.connect_peer((ip, p))

                s = h.status()
                print(f"\tProgress: {s.progress*100:.1f}% | Peers: {s.num_peers} | Down: {s.download_rate/1000:.1f} KB/s")

        # Monitor existing torrents
        for handle in self.t_ses.get_torrents():
            s = handle.status()
            print(f"[{handle.info_hash()}] Progress: {s.progress*100:.1f}%")
            # for a in ses.pop_alerts():
            #     print(f"[alert] {a}")
                
    def on_sample(self, sample):
        print('Received Zenoh message')
        self.message_queue.put(sample)

    # orchestrator callbacks
    def on_torrent_discovered():
        pass

    def on_metata_received():
        pass


# ======================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mutable Peer (LibTorrent + Zenoh)")
    parser.add_argument('-s', '--peer_params', type=str, default = 'peer_params.json')
    parser.add_argument('-q', '--state_file', type=str, default = 'mutable_state.json')
    parser.add_argument('--poll_hz', type=float, default = 0.25)
    args = parser.parse_args()
    robot_id = os.getenv("ROBOT_ID")

    with open(f'torrent/{args.peer_params}', "r") as f:
                params = json.load(f)

    with open(f'torrent/{args.state_file}', "r") as f:
                state = json.load(f)

    mutable_peer = MutablePeer(
        params=params,
        state=state,
        poll_hz=args.poll_hz
    )
    mutable_peer.run()