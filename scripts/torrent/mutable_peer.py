#!/usr/bin/env python3
import libtorrent as lt
import time
import os
import csv
import argparse

from .torrent_utils import *
from .piece_pickers import get_policy, ones_filter, eval_seq
import logging

PORT = 5204
FLEET_SIZE=16
logger = logging.getLogger(__name__)

class MutablePeer:
    """
    Listen to gossip and join torrent sessions
    """
    def __init__(self, params : dict, posegraph : str, state : dict, robot_id, policy, pol_param, t_ses, t_lock, poll_hz : float = 0.2, 
                on_metadata_received=None):
        
        # robot params
        self.container = params['container']
        self.peers = []

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
        
        # mutable updates populated from gossiper callbacks
        self.torrent_handles = {} # robot_id -> handle
        self.known_infohash = {}  # robot_id -> infohash bytes
        self.processed_metadata_hashes = set()  # Track infohashes we've already handled

        # inversion of control callbacks
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
                self._process_alerts()
                self._reconnect_known_peers()
                self._eval_trs()
                time.sleep(1.0 / self.poll_hz)
        except KeyboardInterrupt:
            logging.info("\nstopped")

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

    def _reconnect_known_peers(self):
        """
        periodically ensure that all known peers are connected to
        """
        if not self.peers or not self.torrent_handles:
            return
        with self.t_lock:
            for robot_id, handle in self.torrent_handles.items():
                if not handle.is_valid():
                    continue
                for ip, p in self.peers:
                    handle.connect_peer((ip, p))


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

    def join_torrent(self, robot_id, infohash, peer_ip):
        """
        join a torrent notified by gossip.on_new_item
        """
        logging.info(f"joining torrent for robot_id {robot_id}")
        self._remember_peer(peer_ip)

        self.known_infohash[robot_id] = bytes(infohash)
        with self.t_lock:
                handle = self.t_ses.add_torrent({
                'info_hash': infohash,
                'save_path': self.output_path,
                'flags': lt.torrent_flags.upload_mode | lt.torrent_flags.default_flags
            })
        self.torrent_handles[robot_id] = handle
 
        for ip, p in self.peers:
            logging.info(f"connecting {handle.info_hash()} via {ip}")
            handle.connect_peer((ip, p))

    def update_torrent(self, robot_id, infohash, peer_ip):
        """
        update a torrent based on gossiper.handle_new_item cb
        """
        self._remember_peer(peer_ip)

        old_hash = self.known_infohash.get(robot_id)
        new_hash = bytes(infohash)
        if old_hash == new_hash:
            logging.debug(f"infohash unchanged for robot id {robot_id}")
            return
        self.known_infohash[robot_id] = new_hash

        # remove old handle
        old_handle = self.torrent_handles.get(robot_id)
        if old_handle is not None and old_handle.is_valid():
            logging.debug(f"infohash updated for robot_id {robot_id}, replacing handle")
            self.processed_metadata_hashes.discard(old_hash)
            old_handle.pause()
            with self.t_lock:
                self.t_ses.remove_torrent(old_handle)
            self.torrent_handles.pop(robot_id, None)
        elif old_handle is not None:
            logging.warning(f"stale/invalid handle for robot_id {robot_id}; removing defensively")
            try:
                with self.t_lock:
                    self.t_ses.remove_torrent(old_handle)
            except Exception as e:
                logging.debug(f"remove_torrent on invalid handle failed (may already be gone): {e}")
            self.torrent_handles.pop(robot_id, None)

        # add new handle
        with self.t_lock:
            new_handle = self.t_ses.add_torrent({
                'info_hash': infohash,
                'save_path': self.output_path,
                'flags': lt.torrent_flags.upload_mode | lt.torrent_flags.default_flags
            })
        self.torrent_handles[robot_id] = new_handle
 
        for ip, p in self.peers:
            logging.debug(f"attempting connect_peer to {(ip, p)}")
            new_handle.connect_peer((ip, p))

    def _remember_peer(self, peer_ip):
        peer_endpoint = (peer_ip, PORT)
        if peer_endpoint not in self.peers:
            self.peers.append(peer_endpoint)
            logging.debug(f"added {peer_endpoint} to fleet peer list ({len(self.peers)} knwon)")

    def _handle_metadata_completion(self, handle):
        """
        callback for metadata alerts. process metadata and send to reconstitutor
        """
        # get torrent info (metadata)
        logging.info(f'_handle_metadata_completion')
        info = handle.get_torrent_info()
        infohash_bytes = bytes(info.info_hash().to_bytes())
                
        fs = info.files()
        num_pieces = info.num_pieces()
        num_files = fs.num_files()
        logging.info(f'num_pieces {num_pieces}, num_files {num_files}')
        
        if infohash_bytes in self.processed_metadata_hashes:
            logging.info(f"infohash_bytes {infohash_bytes} in self.processed_metadata_hashes; return")
            return

        # update priorities imediately
        self._on_file_completed(handle)
        # find associated robot_id
        robot_id = None
        for rid, ih in self.known_infohash.items():
            if ih == infohash_bytes:
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

# ======================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mutable Peer (LibTorrent)")
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