from posegraph.live_deconstitution import Deconstitutor
from posegraph.live_reconstitution import Reconstitutor
from torrent.mutable_seeder import MutableSeeder
from torrent.mutable_peer import MutablePeer
from torrent.torrent_utils import unpack_device
from torrent.piece_pickers import (
    get_policy, rarest_random, sequential, cascading, hybrid, sequence_random
)
from utils import *
import logging

import zenoh
import libtorrent as lt
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
        
        # source_pg = f"{VTRTEMP}/pgs/{posegraph}/graph" # input posegraph to dec
        # source_pc = f"{VTRTEMP}/pcs/{posegraph}_{robot_id}/{robot_id}" # output pieces
        # rcv_pc = f"{VTRTEMP}/pcs/{posegraph}_{robot_id}" # received pieces
        # rcv_pg = f"{VTRTEMP}/pgs/r{posegraph}_{robot_id}/graph" # output posegraph from rec
        
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

        # zenoh session for this device
        self.my_ip, z_cfg = unpack_device(seeder_params, self.robot_id)
        z_cfg.insert_json5("open/return_conditions/connect_scouted", "false")
        self.z_ses = zenoh.open(z_cfg)

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

        # from pieces, pub Zenoh, seed torrent
        # dict of {'robot_id' : robot_id, 'seed' : mutable_seeder}
        mutable_seeder = MutableSeeder(
            params=self.seeder_params,
            posegraph=self.posegraph,
            this_robot_id=self.robot_id,
            robot_id=self.robot_id,
            state=state,
            z_ses=self.z_ses,
            t_ses=self.t_ses,
            t_lock=self.t_lock,
            my_ip=self.my_ip
        )
        self.fleet = {}
        self.fleet[self.robot_id] = mutable_seeder
        self.fleet_lock = threading.Lock()
        self.threads = {}

        # discover torrents from zenoh, leech, spawn MutableSeeders
        self.metadata = {}
        self.peer = MutablePeer(
            params=self.peer_params,
            posegraph=self.posegraph,
            state=state,
            robot_id=robot_id,
            policy = self.policy,
            pol_param=self.pol_param,
            z_ses=self.z_ses,
            t_ses=self.t_ses,
            t_lock=self.t_lock,
            my_ip=self.my_ip,
            on_torrent_discovered=self.handle_torrent_discovered,
            on_metadata_received=self.handle_metadata_received,
            on_torrent_updated=self.handle_torrent_update
        )

        # pieces -> posegraph
        self.topology = {}
        self.rec = Reconstitutor(
            pieces_path=rcv_pc,
            robot_id=self.robot_id,
            output_dir=rcv_pg
        )

    def handle_torrent_discovered(self, robot_id, mutable_item):
        # new torrent discovered; spawn a new seeder (cb from mutable_peer)
        logging.info(f"handle_torrent_discovered, id {robot_id}")
        with self.fleet_lock:
            existing_thread = self.threads.get(robot_id)
            if existing_thread is not None and existing_thread.is_alive():
                logging.info(f"Seeder for robot_id {robot_id} already active; ignoring rediscovery")
                return
            if existing_thread is not None:
                logging.warning(f"Stale seeder entry for robot_id {robot_id} found (thread died); respawning")

            mutable_seeder = MutableSeeder(
                params=self.seeder_params,
                posegraph=self.posegraph,
                this_robot_id=self.robot_id,
                robot_id=robot_id,
                mutable_item=mutable_item,
                state=self.state,
                z_ses=self.z_ses,
                t_ses=self.t_ses,
                t_lock=self.t_lock,
                my_ip=self.my_ip
            )
            self.fleet[robot_id] = mutable_seeder
            new_thread = threading.Thread(target=self.fleet[robot_id].run, daemon=True, name=f"SeederThread{robot_id}")
            self.threads[f"seed{robot_id}"] = new_thread
            new_thread.start()

    def handle_metadata_received(self, robot_id, topology):
        # topology update, pass to reconstitutor (cb from mutable_peer)
        logging.info(f"handle_metadata_received for id {robot_id}")
        self.topology[robot_id] = topology
        self.rec.update_topology(robot_id, topology)

    def handle_torrent_update(self, robot_id, mutable_item):
        logging.info(f"handle_torrent_update for id {robot_id}")
        # self.fleet[robot_id].update_mutable_item(mutable_item) # TODO: ANTHONY UNCOMMENT THIS

    def run(self):
        logging.info("[agent.run]")

        with self.fleet_lock:             
            self.threads["dec"] = threading.Thread(target=self.dec.run, daemon=True, name="DeconstitutorThread")
            self.threads[f"seed{self.robot_id}"] = threading.Thread(target=self.fleet[self.robot_id].run, daemon=True, name=f"SeederThread{self.robot_id}")
            self.threads["peer"] = threading.Thread(target=self.peer.run, daemon=True, name="PeerThread")
            self.threads["rec"] =threading.Thread(target=self.rec.run, daemon=True,name="ReconstitutorThread")
            startup_threads = list(self.threads.values())

        for t in startup_threads:
            t.start()

        try:
            while True:
                with self.fleet_lock:
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