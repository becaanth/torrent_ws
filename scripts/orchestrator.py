import logging
import libtorrent as lt
import rclpy
import argparse
import json
import os
import threading
import time 
import faulthandler

from posegraph.live_deconstitution import Deconstitutor
from posegraph.live_reconstitution import Reconstitutor
from posegraph.tr_listener import RobotStateListener
from torrent.mutable_seeder import MutableSeeder
from torrent.mutable_peer import MutablePeer
from torrent.zenoh_gossiper import ZenohGossiper
from torrent.piece_pickers import (
    get_policy, rarest_random, sequential, cascading, hybrid, sequence_random
)
from utils import *

LT_PORT = 5204
CONVOY_ORDER = [0, 1, 2, 3]          # head (teacher) first
ROBOT_ADDRS = {
    0: ["192.168.2.42"],
    1: ["192.168.3.42"],
    2: ["192.168.4.42"],
    3: ["192.168.5.42"],
}

def cascade_ip_filter(robot_id, order=CONVOY_ORDER, addrs=ROBOT_ADDRS):
    """Allow BitTorrent connections only to the robots directly ahead of and behind this one."""
    f = lt.ip_filter()
    f.add_rule("0.0.0.0", "255.255.255.255", 1)                         # block all IPv4
    f.add_rule("::", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", 1)      # block all IPv6
    i = order.index(int(robot_id))
    neighbours = [r for r in order[max(0, i - 1): i + 2] if r != int(robot_id)]
    for r in neighbours:
        for ip in addrs[r]:
            f.add_rule(ip, ip, 0)                                       # allow
    logging.info(f"cascade: robot {robot_id} may connect only to {neighbours}")
    p = lt.port_filter()

    return f

class Orchestrator:
    """
    TTR modules for an agent
    - 1 Deconstitutor
    - 1 MutableSeeder (on spawn), up to N instances
    - 1 MutablePeer (spawns MutableSeeders)
    - 1 Reconstitutor
    """
    def __init__(self, seeder_params: dict, peer_params: dict, state:dict, robot_id: int, posegraph: str, policy: str, pol_param : float):
        VTRTEMP = os.getenv("VTRTEMP")
        
        source_pg = f"{VTRTEMP}/pgs/{posegraph}/graph" # input posegraph to dec
        source_pc = f"{VTRTEMP}/pcs/{posegraph}_{robot_id}/{robot_id}" # output pieces
        rcv_pc = f"{VTRTEMP}/pcs/{posegraph}_{robot_id}" # received pieces
        rcv_pg = f"{VTRTEMP}/pgs/{posegraph}/graph" # output posegraph from rec
        
        self.robot_id = robot_id
        self.posegraph = posegraph
        self.policy = get_policy(policy)
        self.pol_param = pol_param
        self.seeder_params = seeder_params
        self.peer_params = peer_params
        self.state = state

        logging.info(f"init with id {robot_id}, posegraph {posegraph}, policy {policy}")
        # libtorrent session for this device
        self.t_lock = threading.RLock()
        alert_mask = (
            lt.alert_category.status
            | lt.alert_category.file_progress
            | lt.alert_category.storage
            | lt.alert_category.error
            | lt.alert_category.peer
            | lt.alert_category.block_progress   # block_finished_alert: who each 16 KiB block came from
            | lt.alert_category.upload           # block_uploaded_alert: who each block was sent to
        )
        self.t_ses = lt.session({
            "listen_interfaces": f"0.0.0.0:{LT_PORT},[::]:{LT_PORT}",
            "enable_dht": False,
            "enable_outgoing_utp": False,
            "enable_incoming_utp": False,
            "alert_mask": alert_mask,
            "peer_fingerprint": f"-TT{int(robot_id):02d}00-",   # lets other robots name this peer
            "min_reconnect_time": 1,     # s between retries to a peer (default 60): neighbours often join a
            "max_failcount": 1000,       # keep retrying neighbours instead of giving up after 3 refusals
        })
        if policy == 'cascading':
            self.t_ses.set_ip_filter(cascade_ip_filter(robot_id))

        # posegraph -> pieces
        self.dec = Deconstitutor(
            input_dir=source_pg,
            output_dir=source_pc,
            robot_id=self.robot_id,
            poll_hz=4.0
        )

        self.gossiper = ZenohGossiper(
             params=self.seeder_params,
             robot_id=self.robot_id,
             on_new_item=self.handle_new_item,
             on_item_updated=self.handle_item_updated
        )
        self.my_ip = self.gossiper.my_ip

        # authoritative infohash for this seeder
        self.mi = {
            'pubkey': 0,
            'robot_id': self.robot_id,
            'seq': self.state['seq'],
            'infohash': -1,
            'my_ip': self.my_ip,
        }

        # from pieces, seed torrent
        # dict of {'robot_id' : robot_id, 'seed' : mutable_seeder}
        self.seeder = MutableSeeder(
            params=self.seeder_params,
            posegraph=self.posegraph,
            this_robot_id=self.robot_id,
            robot_id=self.robot_id,
            state=state,
            poll_hz=1/11,
            t_ses=self.t_ses,
            t_lock=self.t_lock,
            my_ip=self.my_ip,
            on_snapshot_created=self.handle_snapshot_created
        )

        # discover torrents from zenoh, leech, spawn MutableSeeders
        self.metadata = {}
        self.peer = MutablePeer(
            params=self.peer_params,
            posegraph=self.posegraph,
            state=state,
            robot_id=robot_id,
            policy = self.policy,
            pol_param=self.pol_param,
            poll_hz=1/1,
            t_ses=self.t_ses,
            t_lock=self.t_lock,
            on_metadata_received=self.handle_metadata_received,
            on_file_completed=self.handle_file_completed
        )

        # pieces -> posegraph
        self.topology = {}
        self.rec = Reconstitutor(
            pieces_path=rcv_pc,
            robot_id=self.robot_id,
            output_dir=rcv_pg
        )

        # listen to T&R
        if not rclpy.ok():
            rclpy.init()

        self.current_vtx = 0 # what vertex is this current agent at?
        self.tr_listener = RobotStateListener(on_new_vertex=self.handle_new_vertex)
        self.threads = {}

    # Callbacks
    def handle_new_item(self, robot_id, mutable_item):
        # a new robot announced a mutable item to gossiper, notify peer (cb from gossiper to peer)
        logging.info(f"handle_new_item from robot id {robot_id}")
        self.peer.join_torrent(robot_id, mutable_item['infohash'], mutable_item['my_ip'])

    def handle_file_completed(self, path):
        # logging.info(f"handle file completed: {path}")
        self.rec.mark_file_complete(path)

    def handle_snapshot_created(self, infohash):
        # seeder created a new snapshot (cb from seeder to gossiper)
        self.mi['seq'] += 1
        self.mi['infohash'] = infohash
        evt("snapshot", seq=self.mi['seq'], ih=bytes(infohash).hex(),
            bytes=self.seeder.len_torrent, metadata=self.seeder.len_metadata)
        logging.info(f"seeding mutable item: robot_id={self.mi['robot_id']} seq={self.mi['seq']}")
        self.gossiper._publish_item(self.mi)

    def handle_item_updated(self, robot_id, mutable_item):
        # a known robot updated its seq, notify peer (cb from gossiper to peer)
        logging.info(f"handle_item_updated, robot id {robot_id}, seq {mutable_item['seq']}")
        self.peer.update_torrent(robot_id, mutable_item['infohash'], mutable_item['my_ip'])

    def handle_metadata_received(self, robot_id, topology):
        # topology update, pass to reconstitutor (cb from mutable_peer)
        logging.info(f"handle_metadata_received for id {robot_id}")
        self.topology[robot_id] = topology
        self.rec.update_topology(robot_id, topology)

    def handle_new_vertex(self, vertex_id):
        # localized to new vertex, pass to seeder to filter files in repeat (cb from tr_listener)
        # logging.info(f"localized to {vertex_id}, telling seeder")
        if vertex_id != self.current_vtx:        # robot_state arrives ~10 Hz; log only changes
            evt("vertex", vid=int(vertex_id))
        self.current_vtx = vertex_id
        self.seeder.current_vtx = vertex_id # when convoying
        # self.seeder.current_vtx = int(2**63) # sentinel to disable repeat filter for now

    def _spin_ros(self):
        """ Helper to spin ROS2 thread """
        try:
            rclpy.spin(self.tr_listener)
        except Exception as e:
            logging.error(f"ROS2 spin error: {e}")

    def run(self):
        logging.info("[agent.run]")

        self.threads["dec"] = threading.Thread(target=self.dec.run, daemon=True, name="DeconstitutorThread")
        self.threads["seeder"] = threading.Thread(target=self.seeder.run, daemon=True, name="SeederThread")
        self.threads["gossiper"] = threading.Thread(target=self.gossiper.run, daemon=True, name="GossiperThread")
        self.threads["peer"] = threading.Thread(target=self.peer.run, daemon=True, name="PeerThread")
        self.threads["rec"] =threading.Thread(target=self.rec.run, daemon=True,name="ReconstitutorThread")

        self.threads["listener"] = threading.Thread(target = self._spin_ros, daemon=True, name="RepeatListenerThread")
        startup_threads = list(self.threads.values())

        for t in startup_threads:
            t.start()

        try:
            while True:
                live_threads = list(self.threads.values())
                if not any(t.is_alive() for t in live_threads):
                    logging.warning("All orchestrator threads have exited; shutting down")
                    break
                for t in live_threads:
                    t.join(timeout=1.0)
                # faulthandler.disable(); faulthandler.enable(file=self._fh, all_threads=True)
        except KeyboardInterrupt:
            logging.info("Interrupted, shutting down orchestrator")
        finally:
            self.shutdown()

    def shutdown(self):
        """Clean up ROS 2 node and context."""
        if hasattr(self, 'listener'):
            self.listener.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Teach, Torrent, Repeat Agent")
    parser.add_argument('-p', '--posegraph', required=True,help="Bag name (subdirectory under folder_path)")
    parser.add_argument('-s', '--seeder_params', type=str, default = 'seeder_params.json')
    parser.add_argument('-l', '--peer_params', type=str, default = 'peer_params.json')
    parser.add_argument('-q', '--state_file', type=str, default = 'mutable_state.json')
    parser.add_argument('-r', '--policy', type=str, default = 'rarest-random', help='piece picker options rarest-random, sequential, cascading, hybrid, sequence-random')
    parser.add_argument('-x', '--pol_param', type=float, default = 1.0, help='piece picker param (for hybrid is s, for sequence-random is N)')
    args = parser.parse_args()
    robot_id = os.getenv("ROBOT_ID")
    if not robot_id:
        raise RuntimeError("ROBOT_ID environment variable must be set")
    
    posegraph = args.posegraph
    policy = args.policy
    pol_param = float(args.pol_param)
    setup_logging(robot_id=robot_id, posegraph=posegraph, log_dir="logs")

    try:
        with open(f'torrent/{args.seeder_params}', "r") as f:
                seeder_params = json.load(f)
        with open(f'torrent/{args.peer_params}', "r") as f:
                peer_params = json.load(f)
        with open(f'torrent/{args.state_file}', "r") as f:
                state = json.load(f)

        orchestrator = Orchestrator(
            seeder_params=seeder_params,
            peer_params=peer_params,
            state=state,
            robot_id=robot_id,
            posegraph=posegraph,
            policy = policy,
            pol_param=pol_param
        )

        orchestrator.run()
    except Exception as e:
        logging.exception(f"Fatal error occured during execution: {e}")
    finally:
        flush_logs()