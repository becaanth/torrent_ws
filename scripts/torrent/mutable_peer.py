#!/usr/bin/env python3
import libtorrent as lt
import time
import os
import csv
import argparse

from .torrent_utils import *
from .piece_pickers import get_policy, ones_filter, eval_seq
import json
from utils import evt
import logging

PORT = 5204
FLEET_SIZE=16
PEER_ID_PREFIX = b"-TT"   # orchestrator sets peer_fingerprint "-TT<robot id, 2 digits>00-"
BLOCK_SIZE = 16 * 1024    # libtorrent transfers pieces in 16 KiB blocks
logger = logging.getLogger(__name__)

def _robot_from_pid(pid):
    """Robot id from a libtorrent peer id the orchestrator sets ("-TT0100-..."); None if unknown."""
    try:
       b = bytes(pid.to_bytes())[:8]
       if b.startswith(PEER_ID_PREFIX):
           return int(b[3:5])
    except Exception:
        pass
    return None
logger = logging.getLogger(__name__)

def _peer_robot(pi):
    """Robot id of a connected peer (peer_info)."""
    return _robot_from_pid(pi.pid)

def _alert_addr(alert):
    """Peer IP of a peer alert (bindings expose it as .endpoint, older ones as .ip)."""
    ep = getattr(alert, "endpoint", None) or getattr(alert, "ip", None)
    try:
        return str(ep[0])
    except Exception:
        return str(ep)
  
class MutablePeer:
    """
    Listen to gossip and join torrent sessions
    """
    def __init__(self, params : dict, posegraph : str, state : dict, robot_id, policy, pol_param, t_ses, t_lock, poll_hz : float = 0.2, 
                on_metadata_received=None, on_file_completed=None):
        
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
        self._seen_paths = set()  # piece files already reported once (re-checks re-announce them)

        # exact per-peer transfer totals, counted block by block from libtorrent alerts.
        # Cumulative for the whole run: unlike peer_info counters they survive disconnects
        # and torrent replacement. (dir, peer robot id, peer ip) -> [bytes, blocks]
        self._xfer = {}
        self._xfer_logged = {}      # key -> bytes at the last "xfer" event
        self._piece_len = {}        # infohash -> (piece_length, total_size) for exact last-block sizes

        # inversion of control callbacks
        self.on_metadata_received = on_metadata_received   # pass to Reconstitutor
        self.on_file_completed = on_file_completed   # pass to Reconstitutor

        # metrics
        self.len_metadata = 0
        self.len_torrent = 0
        self.metrics_csv = f"csv/trs_{self.robot_id}_{self.posegraph}_{self.policy.__name__}.csv"
        self._init_metrics_csv()

        # etc
        self.poll_hz = poll_hz


    def _block_bytes(self, alert):
        """Size of the block in this alert: 16 KiB, except a shorter last block of the torrent."""
        try:
            ih = str(alert.handle.info_hash())
            if ih not in self._piece_len:
                ti = alert.handle.torrent_file()
                if ti is None:
                    return BLOCK_SIZE
                self._piece_len[ih] = (ti.piece_length(), ti.total_size())
            plen, total = self._piece_len[ih]
            start = alert.piece_index * plen + alert.block_index * BLOCK_SIZE
            return max(0, min(BLOCK_SIZE, plen - alert.block_index * BLOCK_SIZE, total - start))
        except Exception:
            return BLOCK_SIZE

    def _count_block(self, alert, direction):
        key = (direction, _robot_from_pid(alert.pid), _alert_addr(alert))
        tot = self._xfer.setdefault(key, [0, 0])
        tot[0] += self._block_bytes(alert)
        tot[1] += 1

    def _init_metrics_csv(self):
            """Create CSV file and write headers if it doesn't exist."""
            if not os.path.exists(self.metrics_csv):
                with open(self.metrics_csv, mode='w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        'timestamp', 'info_hash', 'robot_id',
                        'up_all_time', 'down_all_time', 
                        'up_payload_rate', 'down_payload_rate',
                        'sequentiality', 'useful_pieces', 'this_robots_pieces', 'total_pieces',
                        'robustness',
                    ])

    def run(self):
        """
        run the main loop
        """
        logging.info(f"running the main loop (Ctrl-C to stop)")
        reconnect_period = 1.0 / self.poll_hz
        last_sample_time = last_reconnect = time.time()
        try:
            while True:
                self.t_ses.wait_for_alert(500)   # ms; returns early as soon as an alert is queued
                self._process_alerts()
 
                current_time = time.time()
                if current_time - last_reconnect >= reconnect_period:
                    self._reconnect_known_peers()
                    last_reconnect = current_time
 
                if current_time - last_sample_time >= 1.0:
                    try:
                        self._eval_trs()
                    except Exception as e:
                        logging.info(f"couldnt eval_trs because of {e}")
 
                    last_sample_time = current_time # Reset the timer
        except KeyboardInterrupt:
            logging.info("\nstopped")
 
    def _is_current(self, handle):
        """
        True only if this handle is still the live torrent for some robot.
        update_torrent() replaces a robot's torrent whenever it gossips a new snapshot, and
        libtorrent removes the old one asynchronously on its own thread: alerts already
        queued for the old torrent keep arriving, and is_valid() can flip to False between
        any two calls on its handle (t_lock cannot prevent that).
        """
        try:
            if not handle.is_valid():
                return False
            with self.t_lock:
                current = list(self.torrent_handles.values())
            return any(handle == h for h in current)
        except RuntimeError:
            return False

    def _process_alerts(self):
        # pop under the lock, then release it: the seeder and gossiper share t_lock
        with self.t_lock:
            alerts = self.t_ses.pop_alerts()

        for alert in alerts:
            try:
                self._handle_alert(alert)
            except RuntimeError as e:
                # "invalid torrent handle used": the torrent was removed while we were using it
                logging.warning(f"skipping {type(alert).__name__}: torrent removed mid-handling ({e})")
            except Exception:
                logging.exception(f"error handling {type(alert).__name__}; continuing")

    def _handle_alert(self, alert):
        # block transfers: count them (also for torrents already replaced -- the bytes still moved)
        if isinstance(alert, lt.block_uploaded_alert):
            self._count_block(alert, "up")
            return
        if isinstance(alert, lt.block_finished_alert):
            self._count_block(alert, "down")
            return

        logging.info(alert)
        
        # alerts for a torrent that has since been replaced by a newer snapshot: ignore.
        if isinstance(alert, (lt.metadata_received_alert, lt.file_completed_alert)) \
                and not self._is_current(alert.handle):
            logging.info(f"ignoring {type(alert).__name__}: torrent was replaced by a newer snapshot")
            return


        # received metadata
        if isinstance(alert, lt.metadata_received_alert):
            handle = alert.handle
            logging.info(f"metadata received for torrent: {handle.info_hash()}")
            self._handle_metadata_completion(handle)
            with self.t_lock:
                handle.unset_flags(lt.torrent_flags.upload_mode)  # now allow downloading

        # piece/file completed
        elif isinstance(alert, lt.file_completed_alert):
            handle = alert.handle
            file_idx = alert.index  # the file that completed
            logging.info(f"file {file_idx} completed on torrent: {handle.info_hash()}")
            self._report_completed_file(handle, file_idx)
            # Execute picker update on that specific handle
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
            for robot_id, handle in list(self.torrent_handles.items()):
                try:
                    if not handle.is_valid():
                        continue
                    for ip, p in self.peers:
                        handle.connect_peer((ip, p))
                except RuntimeError:
                    continue # being replaced by update_torrent currently

    def _log_xfer(self):
        """
        One "xfer" event per (direction, peer) whose total changed since the last tick.
        bytes/blocks are cumulative since the orchestrator started (never reset), so the
        difference between two events of the same series is exactly the data moved between them.
        """
        for (direction, peer, addr), (nbytes, nblocks) in self._xfer.items():
            key = (direction, peer, addr)
            if self._xfer_logged.get(key) == nbytes:
                continue
            self._xfer_logged[key] = nbytes
            evt("xfer", dir=direction, peer=peer, addr=addr, bytes=nbytes, blocks=nblocks)
 
    def _eval_trs(self):
        self._log_xfer()
        timestamp = time.time()
        with self.t_lock:
            # infohash -> robot whose map this torrent carries; anything else is this robot's own seed
            origin_of = {}
            for rid, h in list(self.torrent_handles.items()):
                try:
                    origin_of[str(h.info_hash())] = rid
                except RuntimeError:
                    pass
            for handle in self.t_ses.get_torrents():
                try:
                    s = handle.status()
                    info_hash = str(handle.info_hash())
                    peer_info = handle.get_peer_info()
 
                    # structured stats for the analysis scripts. Counters are per torrent (they
                    # restart when a snapshot replaces it) and per connection (restart on reconnect).
                    origin = origin_of.get(info_hash, self.robot_id)
                    role = "remote" if info_hash in origin_of else "own"
                    evt("torrent", ih=info_hash, origin=origin, role=role, state=str(s.state),
                        dl=s.total_payload_download, ul=s.total_payload_upload,
                        dl_wire=s.total_download, ul_wire=s.total_upload,
                        dl_rate=s.download_payload_rate, ul_rate=s.upload_payload_rate,
                        have=int(sum(s.pieces)) if s.pieces else 0, pieces=len(s.pieces),
                        peers=s.num_peers)
                    for pi in peer_info:
                        evt("peer", ih=info_hash, origin=origin, role=role,
                            peer=_peer_robot(pi), addr=str(pi.ip[0]),
                            dl=pi.total_download, ul=pi.total_upload,      # payload bytes, this connection
                            dl_rate=pi.payload_down_speed, ul_rate=pi.payload_up_speed)
 
                    # throughput
                    up_all_time = s.all_time_upload
                    down_all_time = s.all_time_download
                    up_payload_rate = s.upload_payload_rate
                    down_payload_rate = s.download_payload_rate
 
                    # sequentiality
                    downloaded_mask = list(s.pieces)
                    sequentiality, U, M, l = eval_seq(downloaded_mask)
 
                    # robustness
                    R = -1 # sentinel value
                    r_bar = 0.0 # defined even with no peers, else the report below never formats
                    p = 0.5 # arbitrary, per paper
 
                    if peer_info:
                        if len(peer_info) >= 0 and M > 0:
                            logging.info(f"peer_info {peer_info}") 
                            peer_matrices = np.array([list(peer.pieces) for peer in peer_info], dtype=int)                                            
                            r_i = np.sum(peer_matrices, axis=0)                         
                            r_bar = np.mean(r_i) 
                            R = float(1.0 - (p ** r_bar))
 
                    eval_string = (
                    f"Eval report for handle {info_hash}\n"
                    f" Throughput Up: {up_payload_rate / 1e6:.2f} MB/s (Total: {up_all_time / 1e6:.2f} MB)\n"
                    f" Throughput Down: {down_payload_rate / 1e6:.2f} MB/s (Total: {down_all_time / 1e6:.2f} MB)\n"
                    f" Sequentiality: {sequentiality:.4f} ({U}/{M} useful pieces)\n"
                    f" Robustness: {R:.4f} (r_bar={r_bar:.2f})"
                    )                        
                    logging.info(eval_string)
 
                    with open(self.metrics_csv, mode='a', newline='') as f:
                        writer = csv.writer(f)
                        writer.writerow([
                            timestamp, info_hash, self.robot_id,
                            up_all_time, down_all_time,
                            up_payload_rate, down_payload_rate,
                            sequentiality, U, M, l, 
                            R,
                        ])
                except Exception as e:
                    logging.info(f"Eval report is not ready bc {e}")


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
            for ip, p in self.peers:
                logging.info(f"connecting {handle.info_hash()} via {ip}")
                handle.connect_peer((ip, p))
            self.torrent_handles[robot_id] = handle

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
        self.processed_metadata_hashes.discard(old_hash)
        with self.t_lock:
            old_handle = self.torrent_handles.pop(robot_id, None)
            if old_handle is not None:
                logging.debug(f"infohash updated for robot_id {robot_id}, replacing handle")
                try:
                    if old_handle.is_valid():
                        old_handle.pause()
                    self.t_ses.remove_torrent(old_handle)
                except RuntimeError as e:
                    logging.debug(f"old handle for robot_id {robot_id} already gone: {e}")

        # add new handle
        with self.t_lock:
            new_handle = self.t_ses.add_torrent({
                'info_hash': infohash,
                'save_path': self.output_path,
                'flags': lt.torrent_flags.upload_mode | lt.torrent_flags.default_flags
            })
            for ip, p in self.peers:
                logging.debug(f"attempting connect_peer to {(ip, p)}")
                new_handle.connect_peer((ip, p))
            self.torrent_handles[robot_id] = new_handle

    def _report_completed_file(self, handle, file_idx):
        """tell Rec. the file is fully downloaded; log first completions as new data"""
        ti = handle.torrent_file()
        if ti is None:
            return

        fs = ti.files()
        rel_path = fs.file_path(file_idx)
        path = os.path.join(self.output_path, rel_path)
 
        # every re-check after a snapshot switch re-announces files already on disk:
        # only the first completion of a path is newly downloaded data
        if path not in self._seen_paths and rel_path.endswith(".db3"):
            self._seen_paths.add(path)
            parts = rel_path.replace(os.sep, "/").split("/")
            evt("piece", ih=str(handle.info_hash()), file=parts[-1],
                origin=parts[-2] if len(parts) > 1 else None, bytes=fs.file_size(file_idx))
 
        if self.on_file_completed is not None:
            self.on_file_completed(path)

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
        logging.info(f"_handle_metadata_completion: robot_id is {robot_id}")

        metadata = info.metadata()
        if metadata:
            self.len_metadata = len(metadata)
            evt("metadata", ih=infohash_bytes.hex(), origin=robot_id, bytes=len(metadata),
                pieces=num_pieces, files=num_files)
            logging.info(f"Received metadata for {infohash_bytes.hex()} | Size: {self.len_metadata} bytes")

        # orchestrator callback
        if metadata and self.on_metadata_received:
            logging.info(f"poll_metadata updated for robot {robot_id}")
            self.on_metadata_received(robot_id, metadata) # -> topology goes to Reconstitutor
            self.processed_metadata_hashes.add(infohash_bytes)     
        else:
            logging.info(f"metadata is None")

    def _on_file_completed(self, handle):
        """
        Triggered when a file is completed downloading
        - reassign piece priorities for that handle according to the policy
        """
        # this handle is
        with self.t_lock:
            try:
                if not handle.is_valid():
                    return
                
                logging.info(f"applying {self.policy.__name__} policy  to handle {handle.info_hash()}")
                priorities = handle.get_piece_priorities() # pieces same as files
                downloaded_mask = list(handle.status().pieces)
                new_priorities = self.policy(priorities, downloaded_mask, self.pol_param)
                filtered_priorities = ones_filter(new_priorities)
                lt_priorities = filtered_priorities.astype(int).tolist()
                check = handle.get_piece_priorities() # pieces same as files
                handle.prioritize_pieces(lt_priorities)
                logging.info(f"priorities: \n{handle.get_piece_priorities()}")
            except RuntimeError as e:
                # torrent removed by update_torrent between the checks above and these calls
                logging.warning(f"_on_file_completed: handle went invalid mid-update ({e}); skipping")

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