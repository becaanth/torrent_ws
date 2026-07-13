from posegraph.live_deconstitution import Deconstitutor
from posegraph.live_reconstitution import Reconstitutor
from torrent.mutable_seeder import MutableSeeder
from torrent.mutable_peer import MutablePeer

import argparse
import json
import os

import threading
import logging
import time

import pdb

class Orchestrator:
    """
    TTR modules for an agent
    - 1 Deconstitutor
    - 1 MutableSeeder (on spawn), up to N instances
    - 1 MutablePeer (spawns MutableSeeders)
    - 1 Reconstitutor
    """
    def __init__(self, seeder_params: dict, peer_params: dict, state:dict, robot_id: int, posegraph: str):
        VTRTEMP = os.getenv("VTRTEMP")
        source_pg = f"{VTRTEMP}/pgs/{posegraph}/graph" # input posegraph to dec
        source_pc = f"{VTRTEMP}/pcs/{posegraph}_{robot_id}/{robot_id}" # output pieces
        rcv_pc = f"{VTRTEMP}/pcs/{posegraph}_{robot_id}" # received pieces
        rcv_pg = f"{VTRTEMP}/pgs/r{posegraph}_{robot_id}/graph" # output posegraph from rec
        self.robot_id = robot_id

        print(f"[orch] init with id {robot_id}, posegraph {posegraph}")

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
            params=seeder_params,
            robot_id=self.robot_id,
            state=state
        )
        self.fleet = {}
        self.fleet[self.robot_id] = mutable_seeder

        # discover torrents from zenoh, leech, spawn MutableSeeders
        self.metadata = {}
        self.peer = MutablePeer(
            params=peer_params,
            state=state,
            robot_id=robot_id,
            on_torrent_discovered=self.handle_torrent_discovered,
            on_metadata_received=self.handle_metadata_received
        )

        # pieces -> posegraph
        self.topology = {}
        self.rec = Reconstitutor(
            pieces_path=rcv_pc,
            robot_id=self.robot_id,
            output_dir=rcv_pg
        )

    def handle_metadata_received(self, robot_id, topology):
        # topology update, pass to reconstitutor (cb from mutable_peer)
        print(f"[orch] handle_metadata_received for id {robot_id}")
        self.topology[robot_id] = topology
        self.rec.update_topology(robot_id, topology)

    def handle_torrent_discovered(self, robot_id):
        # new torrent discovered; spawn a new seeder (cb from mutable_peer)
        print(f"[orch] handle_torrent_discovered, id {robot_id}")
        mutable_seeder = MutableSeeder(
            params=seeder_params,
            robot_id=robot_id,
            state=state
        )
        self.fleet[robot_id] = mutable_seeder
        new_thread = threading.Thread(target=self.fleet[robot_id].run, daemon=True)
        new_thread.start()

    def run(self):
        logging.info("[agent.run]")
        dec_thread = threading.Thread(target=self.dec.run, daemon=True)
        dec_thread.start()
        seed_thread = threading.Thread(target=self.fleet[self.robot_id].run, daemon=True)
        seed_thread.start()
        peer_thread = threading.Thread(target=self.peer.run, daemon=True)
        peer_thread.start()
        rec_thread  = threading.Thread(target=self.rec.run, daemon=True)

        rec_thread.start()

        dec_thread.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Teach, Torrent, Repeat Agent")
    parser.add_argument('-p', '--posegraph', required=True,help="Bag name (subdirectory under folder_path)")
    parser.add_argument('-s', '--seeder_params', type=str, default = 'seeder_params.json')
    parser.add_argument('-l', '--peer_params', type=str, default = 'peer_params.json')
    parser.add_argument('-q', '--state_file', type=str, default = 'mutable_state.json')
    args = parser.parse_args()
    robot_id = os.getenv("ROBOT_ID")

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
        posegraph=args.posegraph
    )

    orchestrator.run()