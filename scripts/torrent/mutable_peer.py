#!/usr/bin/env python3
import libtorrent as lt
import time
import zenoh
from queue import Queue
from collections import deque
import os
import csv
import argparse

from .torrent_utils import *
from .piece_pickers import get_policy, ones_filter, eval_seq
import logging

import pdb

PORT = 5204
FLEET_SIZE=16
logger = logging.getLogger(__name__)

class MutablePeer:
    """
    Listen to Zenoh gossip, join a torrent session
    """
    def __init__(self, params : dict, posegraph : str, state : dict, robot_id, policy, pol_param, z_ses, t_ses, t_lock, my_ip, poll_hz : float = 0.5,
                 on_torrent_discovered=None, on_metadata_received=None, on_torrent_updated=None):
        
        # robot params
        self.container = params['container']
        self.peers = []

        self.my_ip = my_ip
        self.z_ses = z_ses

        logging.info("unpacked config")
        self.router = params['router']
        self.robot_id = robot_id
        self.policy = policy
        self.pol_param = pol_param

        # data params
        self.posegraph = posegraph
        self.state = state

        # file system
        self.output_path = f"{os.getenv('VTRTEMP')}/pcs/{self.posegraph}_{self.robot_id}"
        logging.info(f"outputh path: {self.output_path}")
        os.makedirs(self.output_path, exist_ok=True)

        # libtorrent
        logging.info("init lt")
        self.t_ses = t_ses
        self.t_lock = t_lock

        # zenoh
        logging.info("init zenoh")

        self.message_queue = Queue()
        self.sub = self.z_ses.declare_subscriber("mutable_items/**", self.on_sample)
        self.last_sample = None
        
        # mutable updates
        self.mutable_items = {}
        self.torrent_handles = {}
        self.processed_metadata_hashes = set()  # Track infohashes we've already handled
        self.max_seq_seen = {} 

        # inversion of control callbacks
        self.on_torrent_discovered = on_torrent_discovered # STALE! spawn MutableSeeder 
        self.on_torrent_updated = on_torrent_updated # STALE! spawn MutableSeeder 
        self.on_metadata_received = on_metadata_received   # pass to Reconstitutor

        self.metrics_csv = f"csv/trs_{self.robot_id}_{self.posegraph}_{self.policy.__name__}.csv"
        self._init_metrics_csv()

        # etc
        self.poll_hz = poll_hz

    def _init_metrics_csv(self):
            """Create CSV file and write headers if it doesn't exist."""
            if not os.path.exists(self.metrics_csv):
                with open(self.metrics_csv, mode='w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        'timestamp', 'info_hash', 'robot_id',
                        'up_all_time', 'down_all_time', 
                        'up_payload_rate', 'down_payload_rate',
                        'sequentiality', 'useful_pieces', 'this_robots_pieces', 'total_pieces'
                        'robustness'
                    ])

    def run(self):
        """
        run the main loop
        """
        logging.info(f"running the main loop (Ctrl-C to stop)")
        try:
            while True:
                logging.debug(f"len queue = {self.message_queue.qsize()}, peers {self.peers}")
                self._process_alerts()
                self._flush_queue()
                self._rebroadcast_known_items()
                self._eval_trs()
                time.sleep(1.0 / self.poll_hz)
        except KeyboardInterrupt:
            logging.info("\nstopped")
        finally:
            self.z_ses.close()

    def _process_alerts(self):
        # Monitor existing torrents
        with self.t_lock:
            alerts = self.t_ses.pop_alerts()
        
        for alert in alerts:
            # received metadata
            logging.info(alert)
            if isinstance(alert, lt.metadata_received_alert):
                handle = alert.handle  # Direct handle reference attached to the alert!
                logging.info(f"metadata received for torrent: {handle.info_hash()}")
                self._handle_metadata_completion(handle)
                handle.unset_flags(lt.torrent_flags.upload_mode)  # now allow downloading

            # piece/file completed
            elif isinstance(alert, lt.file_completed_alert):
                handle = alert.handle  # Direct handle reference!
                try:
                    file_idx = alert.index # The file/piece index that completed
                    logging.info(f"file {file_idx} completed on torrent: {handle.info_hash()}")
                except:
                    logging.info(f"file_idx alert corrupted")
            
                # Directly execute your policy update on that specific handle
                self._on_file_completed(handle)
            
            # connection/debug            
            elif isinstance(alert, (lt.peer_connect_alert, lt.peer_disconnected_alert, lt.peer_error_alert)):
                logging.debug(f"peer event: {alert}")

    def _eval_trs(self):
        timestamp = time.time()
        with self.t_lock:
            for handle in self.t_ses.get_torrents():
                s = handle.status()

                try:
                    # throughput
                    up_all_time = s.all_time_upload
                    down_all_time = s.all_time_download
                    up_payload_rate = s.upload_payload_rate
                    down_payload_rate = s.download_payload_rate

                    # sequentiality
                    downloaded_mask = list(s.pieces)
                    sequentiality, U, M, l = eval_seq(downloaded_mask)

                    # robustness
                    peer_info = handle.get_peer_info()
                    R = 0
                    p = 0.5 # arbitrary, per paper
                    if len(peer_info) >= 0 and M > 0:
                        logging.info(f"peer_info {peer_info}") 
                        peer_matrices = np.array([list(peer.pieces) for peer in peer_info], dtype=int)                                            
                        r_i = np.sum(peer_matrices, axis=0)                         
                        r_bar = np.mean(r_i) 
                        R = float(1.0 - (p ** r_bar))

                    eval_string = (
                    f"Eval report for handle {handle.info_hash()}\n"
                    f" Throughput Up: {up_payload_rate / 1e6:.2f} MB/s (Total: {up_all_time / 1e6:.2f} MB)\n"
                    f" Throughput Down: {down_payload_rate / 1e6:.2f} MB/s (Total: {down_all_time / 1e6:.2f} MB)\n"
                    f" Sequentiality: {sequentiality:.4f} ({U}/{M} useful pieces)\n"
                    f" Robustness: {R:.4f} (r_bar={r_bar:.2f})"
                    )                        
                    logging.info(eval_string)

                    with open(self.metrics_csv, mode='a', newline='') as f:
                        writer = csv.writer(f)
                        writer.writerow([
                            timestamp, str(handle.info_hash()), self.robot_id,
                            up_all_time, down_all_time,
                            up_payload_rate, down_payload_rate,
                            sequentiality, U, M, l, 
                            R
                        ])
                except:
                    logging.info("Eval report is not ready ")

    def _flush_queue(self):
        """
        flush self.message_queue
        """
        # Check for new messages (non-blocking)
        while not self.message_queue.empty():
            sample = self.message_queue.get()
            mutable_item = on_mutable_item(sample)
            logging.info(f"mutable item received \n {mutable_to_string(mutable_item)}")

            robot_id = mutable_item['robot_id']
            seq = mutable_item['seq']

            if robot_id == self.robot_id:
                logging.debug(f"skipping gossip about our own robot_id {self.robot_id}")
                continue

            if mutable_item['my_ip'] == self.my_ip:
                logging.debug(f"skipping gossip from our own seeder")
                continue

            # check for freshness
            last_seq = self.max_seq_seen.get(robot_id, -1)
            if seq <= last_seq:
                logging.debug(f"ignoring stale seq {seq} for robot_id {robot_id}, have seq {last_seq}")
                continue
            self.max_seq_seen[robot_id] = seq

            peer_endpoint = (mutable_item['my_ip'], PORT)
            if peer_endpoint not in self.peers:
                self.peers.append(peer_endpoint)
                logging.debug(f"added {peer_endpoint} to fleet peer list ({len(self.peers)} known)")
            
            # check if new session discovered
            existing_ids = {mi['robot_id'] for _, mi in self.mutable_items.items() if 'robot_id' in mi}
            robot_id = mutable_item['robot_id']
            if robot_id not in existing_ids:
                logging.info(f"new \n{mutable_to_string(mutable_item)}")
                self.mutable_items[robot_id] = mutable_item
                with self.t_lock:
                    handle = self.t_ses.add_torrent({
                        'info_hash': mutable_item['infohash'],
                        'save_path': self.output_path,
                        'flags': lt.torrent_flags.upload_mode | lt.torrent_flags.default_flags
                    })
                self.torrent_handles[robot_id] = handle

                # connect handle to known peers
                for ip, p in self.peers:
                    handle.connect_peer((ip, p))

                # orchestrator callbacks
                if self.on_torrent_discovered is not None:
                    self.on_torrent_discovered(robot_id, mutable_item) # -> STALE new session, spawn MutableSeeder

            # if new infohash, remove old torrent session
            else:
                saved_mi = self.mutable_items[robot_id]
                logging.debug(f"overwrite \n\t {mutable_to_string(mutable_item)}")
                self.mutable_items[robot_id] = mutable_item

                # enforce robots authority on local sessions
                if self.on_torrent_updated is not None and mutable_item['robot_id']!=self.robot_id:
                    self.on_torrent_updated(robot_id, mutable_item)

                # remove old torrent
                old_handle = self.torrent_handles.get(robot_id)

                if old_handle is not None and old_handle.is_valid():
                    old_hash = bytes(old_handle.info_hash().to_bytes())
                    new_hash = bytes(mutable_item['infohash'])

                    if old_hash == new_hash:
                        # infohash has not changes
                        continue

                    logging.debug(f"infohash updated for robot_id {robot_id}, replacing handle")
                    self.processed_metadata_hashes.discard(old_hash)
                    old_handle.pause()
                    with self.t_lock:
                        self.t_ses.remove_torrent(old_handle)
                    self.torrent_handles.pop(robot_id, None)
                    
                with self.t_lock:
                    new_handle = self.t_ses.add_torrent({
                        'info_hash': mutable_item['infohash'],
                        'save_path': self.output_path,
                        'flags': lt.torrent_flags.upload_mode | lt.torrent_flags.default_flags
                    })
                self.torrent_handles[robot_id] = new_handle

                for ip, p in self.peers:
                    logging.debug(f"attempting connect_peer to {(ip, p)}")
                    new_handle.connect_peer((ip, p))

    def _forward_gossip(self, mutable_item):
        """
        instead of spawning a new seeder, creating conflicts at the snapshot level, use libtorrent to handle multi-seeding
        """
        relay_mi = dict(mutable_item)
        relay_mi['my_ip'] = self.my_ip
        payload = msgpack.packb(relay_mi, use_bin_type=True)
        logging.debug(f"forwarding gossip for robot_id {relay_mi['robot_id']} seq {relay_mi['seq']}")
        self.z_ses.put(f"mutable_items/{relay_mi['robot_id']}", payload)

    def _rebroadcast_known_items(self):
        """
        periodically re-announce everything currently known
        """
        logging.debug(f"rebroadcasting known items")
        for robot_id, mutable_item in self.mutable_items.items():
            self._forward_gossip(mutable_item)

    def _handle_metadata_completion(self, handle):
        """
        callback for metadata alerts. process metadata and send to reconstitutor
        """
        # get torrent info (metadata)
        info = handle.get_torrent_info()
        infohash_bytes = bytes(info.info_hash().to_bytes())
                
        fs = info.files()
        num_pieces = info.num_pieces()
        num_files = fs.num_files()
        logging.info(f'num_pieces {num_pieces}, num_files {num_files}')
        
        if infohash_bytes in self.processed_metadata_hashes:
            return

        # update priorities imediately
        self._on_file_completed(handle)
        # find associated robot_id
        robot_id = None
        for rid, mi in self.mutable_items.items():
            mi_hash =  mi['infohash']
            if isinstance(mi_hash, (bytearray, memoryview)):
                mi_hash = bytes(mi_hash)

            if mi_hash == infohash_bytes:
                robot_id = rid
                break
        
        if robot_id is None:
            return

        metadata = info.metadata()

        # orchestrator callback
        if metadata and self.on_metadata_received:
            logging.info(f"poll_metadata updated for robot {robot_id}")
            self.on_metadata_received(robot_id, metadata) # -> topology goes to Reconstitutor
            self.processed_metadata_hashes.add(infohash_bytes)               

    def _on_file_completed(self, handle):
        """
        Triggered when a file is completed downloading
        - reassign piece priorities for that handle according to the policy
        """
        # this handle is
        with self.t_lock:
            if not handle.is_valid():
                return
            
            logging.info(f"applying {self.policy.__name__} policy  to handle {handle.info_hash()}")
            priorities = handle.get_piece_priorities() # pieces same as files
            downloaded_mask = list(handle.status().pieces)
            new_priorities = self.policy(priorities, downloaded_mask, self.pol_param)
            filtered_priorities = ones_filter(new_priorities)
            lt_priorities = filtered_priorities.astype(int).tolist()
            check = handle.get_piece_priorities() # pieces same as files
            logging.info(f"priorities: \n{check}")
            handle.prioritize_files(lt_priorities)

    def on_sample(self, sample):
        logging.info("Received Zenoh message")
        
        if sample != self.last_sample:
            self.message_queue.put(sample)
            self.last_sample = sample
        else:
            logging.debug(f"Duplicate sample received")

# ======================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mutable Peer (LibTorrent + Zenoh)")
    parser.add_argument('-s', '--peer_params', type=str, default = 'peer_params.json')
    parser.add_argument('-p', '--posegraph', required=True,help="Bag name (subdirectory under folder_path)")
    parser.add_argument('-q', '--state_file', type=str, default = 'mutable_state.json')
    parser.add_argument('--poll_hz', type=float, default = 0.25)
    parser.add_argument('-r', '--policy', type=str, default = 'rarest-random', help='piece picker options rarest-random, sequential, cascading, hybrid, sequence-random')
    args = parser.parse_args()
    robot_id = os.getenv("ROBOT_ID")
    posegraph=args.posegraph
    policy = get_policy(args.policy)

    with open(f'torrent/{args.peer_params}', "r") as f:
                params = json.load(f)

    with open(f'torrent/{args.state_file}', "r") as f:
                state = json.load(f)

    mutable_peer = MutablePeer(
        params=params,
        posegraph=posegraph,
        state=state,
        robot_id=robot_id,
        policy=policy,
        poll_hz=args.poll_hz
    )
    mutable_peer.run()