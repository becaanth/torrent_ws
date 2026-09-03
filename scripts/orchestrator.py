from posegraph.live_deconstitution import Deconstitutor
from posegraph.live_reconstitution import Reconstitutor
from torrent.mutable_seeder import MutableSeeder
from torrent.mutable_peer import MutablePeer
from torrent.zenoh_gossiper import ZenohGossiper
from torrent.piece_pickers import (
    get_policy, rarest_random, sequential, cascading, hybrid, sequence_random
)
from utils import *
import logging

import libtorrent as lt
from nacl.signing import SigningKey
import argparse
import json
import os

import threading

LT_PORT = 5204

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
        self.t_lock = threading.Lock()
        alert_mask = (
            lt.alert_category.status
            | lt.alert_category.file_progress
            | lt.alert_category.storage
            | lt.alert_category.error
            | lt.alert_category.peer
        )
        self.t_ses = lt.session({
            "listen_interfaces": f"0.0.0.0:{LT_PORT},[::]:{LT_PORT}",
            "enable_dht": False,
            "enable_outgoing_utp": False,
            "enable_incoming_utp": False,
            "alert_mask": alert_mask
        })

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
        sk = SigningKey(bytes.fromhex(self.state["sk"]))
        self.mi = {
            'pubkey': sk.verify_key.encode(),
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
            t_ses=self.t_ses,
            t_lock=self.t_lock,
            on_metadata_received=self.handle_metadata_received,
        )

        # pieces -> posegraph
        self.topology = {}
        self.rec = Reconstitutor(
            pieces_path=rcv_pc,
            robot_id=self.robot_id,
            output_dir=rcv_pg
        )

        self.threads = {}

    # Callbacks
    def handle_new_item(self, robot_id, mutable_item):
        # a new robot announced a mutable item to gossiper, notify peer (cb from gossiper to peer)
        logging.info(f"handle_new_item from robot id {robot_id}")
        self.peer.join_torrent(robot_id, mutable_item['infohash'], mutable_item['my_ip'])

    def handle_snapshot_created(self, infohash):
        # seeder created a new snapshot (cb from seeder to gossiper)
        self.mi['seq'] += 1
        self.mi['infohash'] = infohash
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

    def run(self):
        logging.info("[agent.run]")

        self.threads["dec"] = threading.Thread(target=self.dec.run, daemon=True, name="DeconstitutorThread")
        self.threads["seeder"] = threading.Thread(target=self.seeder.run, daemon=True, name="SeederThread")
        self.threads["gossiper"] = threading.Thread(target=self.gossiper.run, daemon=True, name="GossiperThread")
        self.threads["peer"] = threading.Thread(target=self.peer.run, daemon=True, name="PeerThread")
        self.threads["rec"] =threading.Thread(target=self.rec.run, daemon=True,name="ReconstitutorThread")
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
        except KeyboardInterrupt:
            logging.info("Interrupted, shutting down orchestrator")

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