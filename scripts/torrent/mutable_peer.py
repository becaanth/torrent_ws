#!/usr/bin/env python3
import libtorrent as lt
import time
import zenoh
from queue import Queue
from collections import deque
import os
import argparse

from .torrent_utils import *

import pdb

PORT = 5204

class MutablePeer:
    """
    Listen to Zenoh gossip, join a torrent session
    """
    def __init__(self, params : dict, posegraph : str, state : dict, robot_id, z_ses, t_ses, my_ip, poll_hz : float = 0.5,
                 on_torrent_discovered=None, on_metadata_received=None, on_torrent_updated=None):
        
        # robot params
        self.container = params['container']
        self.peers = []

        self.my_ip = my_ip
        self.z_ses = z_ses

        print("[Peer]: unpacked config")
        self.router = params['router']
        self.robot_id = robot_id

        # data params
        self.posegraph = posegraph
        self.state = state

        # file system
        self.output_path = f"{os.getenv('VTRTEMP')}/pcs/{self.posegraph}_{self.robot_id}"
        print(f"[Peer]: outputh path: {self.output_path}")
        os.makedirs(self.output_path, exist_ok=True)

        # libtorrent
        print("[Peer]: init lt")
        self.t_ses = t_ses

        # zenoh
        print("[Peer]: init zenoh")

        self.message_queue = Queue()
        self.sub = self.z_ses.declare_subscriber("mutable_items/**", self.on_sample)
        self.last_sample = None
        
        # mutable updates
        self.mutable_items = {}

        # inversion of control callbacks
        self.on_torrent_discovered = on_torrent_discovered # spawn MutableSeeder 
        self.on_torrent_updated = on_torrent_updated # spawn MutableSeeder 
        self.on_metadata_received = on_metadata_received   # pass to Reconstitutor

        # etc
        self.poll_hz = poll_hz

    def run(self):
        """
        run the main loop
        """
        print(f"[Peer]: running the main loop (Ctrl-C to stop)")
        try:
            while True:
                print(f"[Peer]: len queue = {self.message_queue.qsize()}, peers {self.peers}")
                self._flush_queue()
                self._poll_metadata()
                time.sleep(1.0 / self.poll_hz)
        except KeyboardInterrupt:
            print("\n[Peer]: stopped")
        finally:
            self.z_ses.close()

    def _flush_queue(self):
        """
        flush self.message_queue
        """
        # Check for new messages (non-blocking)
        while not self.message_queue.empty():
            sample = self.message_queue.get()
            
            mutable_item = on_mutable_item(sample)
            print(f"[Peer]: mutable item received \n {mutable_to_string(mutable_item)}")

            # check if heard own seeder
            if mutable_item['my_ip'] == self.my_ip:
                print(f"[Peer]: WARNING flush skipping. don't leech own pieces")
                continue

            peer_endpoint = (mutable_item['my_ip'], PORT)
            if peer_endpoint not in self.peers:
                self.peers.append(peer_endpoint)
                print(f"[Peer]: added {peer_endpoint} to fleet peer list ({len(self.peers)} known)")
            
            # check if new session discovered
            existing_ids = {mi['robot_id'] for _, mi in self.mutable_items.items() if 'robot_id' in mi}
            robot_id = mutable_item['robot_id']
            if robot_id not in existing_ids:
                print(f"[Peer]: new \n{mutable_to_string(mutable_item)}")
                self.mutable_items[robot_id] = mutable_item
                handle = self.t_ses.add_torrent({
                    'info_hash': mutable_item['infohash'],
                    'save_path': self.output_path
                })
                for ip, p in self.peers:
                    handle.connect_peer((ip, p))

                for h in self.t_ses.get_torrents():
                    if h.info_hash() != handle.info_hash():
                        h.connect_peer(peer_endpoint)

                # orchestrator callbacks
                if self.on_torrent_discovered is not None:
                    self.on_torrent_discovered(robot_id, mutable_item) # -> new session, spawn MutableSeeder

            # if new infohash, remove old torrent session
            else:
                saved_mi = self.mutable_items[robot_id]
                if mutable_item['seq'] > saved_mi['seq']: # what if we torrent a completed map? then these are equal
                    # overwrite mutable item
                    print(f"[Peer]: overwrite \n\t {mutable_to_string(mutable_item)}")
                    self.mutable_items[robot_id] = mutable_item

                    # enforce robots authority on local sessions
                    if self.on_torrent_updated is not None and mutable_item['robot_id']!=self.robot_id:
                        self.on_torrent_updated(robot_id, mutable_item)

                    # remove old torrent
                    for active_handle in self.t_ses.get_torrents():
                        try:                        
                            if active_handle.get_torrent_info().info_hash().to_bytes() == saved_mi['infohash']:
                                self.t_ses.remove_torrent(active_handle)
                                handle = self.t_ses.add_torrent({
                                    'info_hash': mutable_item['infohash'],
                                    'save_path': self.output_path
                                })
                                for ip, p in self.peers:
                                    print(f"[Peer]: attempting connect_peer to {peer_endpoint}")
                                    handle.connect_peer(peer_endpoint)
                                    handle.connect_peer((ip, p))
                        except:
                            print("[Peer]: no torrent info")

        # Monitor existing torrents
        for handle in self.t_ses.get_torrents():
            s = handle.status()
            print(f"[Peer]: progress {s.progress*100:.1f}%")
            for a in self.t_ses.pop_alerts():
                if isinstance(a, (lt.peer_connect_alert, lt.peer_disconnected_alert,
                                lt.peer_error_alert, lt.metadata_failed_alert,
                                lt.metadata_received_alert)):
                    print(f"[Peer] alert: {a}")

        # for _, mi in self.mutable_items.items():
        #     print(f"[Peer]: item \n{mutable_to_string(mi)})")

    def _poll_metadata(self):
        """
        torrent sessions don't immediately have metadata available.
        waiting in flush_queue blocks the thread
        """
        for handle in self.t_ses.get_torrents():
            if handle.has_metadata():
                # get torrent info (metadata)
                info = handle.get_torrent_info()
                metadata = info.metadata()
                # find associated robot_id
                infohash = info.info_hash().to_bytes()
                for rid, mi in self.mutable_items.items():
                    if mi['infohash']==infohash:
                        robot_id = rid
                        break

                # orchestrator callback
                if self.on_metadata_received is not None:
                    if robot_id is not None or metadata is not None:
                        self.on_metadata_received(robot_id, metadata) # -> topology goes to Reconstitutor
                    
                    print(f"[Peer] poll_metadata updated for robot {robot_id}")

    def on_sample(self, sample):
        print("[Peer]: Received Zenoh message")
        if sample != self.last_sample:
            self.message_queue.put(sample)
            self.last_sample = sample
        else:
            print(f"[Peer]: Duplicate sample received")

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
        robot_id=robot_id,
        poll_hz=args.poll_hz
    )
    mutable_peer.run()