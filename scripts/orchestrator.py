from posegraph.live_deconstitution import Deconstitutor
from posegraph.live_reconstitution import Reconstitutor
from torrent.mutable_seeder import MutableSeeder
from torrent.mutable_peer import MutablePeer
from torrent.torrent_utils import unpack_device

import zenoh
import argparse
import json
import os

import threading
import logging

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

        print(f"[Orchestrator]: init with id {robot_id}, posegraph {posegraph}")

        # zenoh session for this device
        self.my_ip, z_cfg = unpack_device(seeder_params, self.robot_id)
        z_cfg.insert_json5("open/return_conditions/connect_scouted", "false")
        self.z_ses = zenoh.open(z_cfg)

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
            posegraph=self.posegraph,
            this_robot_id=self.robot_id,
            robot_id=self.robot_id,
            state=state,
            z_ses=self.z_ses,
            my_ip=self.my_ip
        )
        self.fleet = {}
        self.fleet[self.robot_id] = mutable_seeder

        # discover torrents from zenoh, leech, spawn MutableSeeders
        self.metadata = {}
        self.peer = MutablePeer(
            params=peer_params,
            posegraph=self.posegraph,
            state=state,
            robot_id=robot_id,
            z_ses=self.z_ses,
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
        print(f"[Orchestrator]: handle_torrent_discovered, id {robot_id}")
        mutable_seeder = MutableSeeder(
            params=seeder_params,
            posegraph=self.posegraph,
            this_robot_id=self.robot_id,
            robot_id=robot_id,
            mutable_item=mutable_item,
            state=state,
            z_ses=self.z_ses,
            my_ip=self.my_ip
        )
        self.fleet[robot_id] = mutable_seeder
        new_thread = threading.Thread(target=self.fleet[robot_id].run, daemon=True)
        new_thread.start()

    def handle_metadata_received(self, robot_id, topology):
        # topology update, pass to reconstitutor (cb from mutable_peer)
        print(f"[Orchestrator]: handle_metadata_received for id {robot_id}")
        self.topology[robot_id] = topology
        self.rec.update_topology(robot_id, topology)

    def handle_torrent_update(self, robot_id, mutable_item):
        print(f"[Orchestrator]: handle_torrent_update for id {robot_id}")
        self.fleet[robot_id].update_mutable_item(mutable_item)

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