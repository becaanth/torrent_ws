import sqlite3
import pandas as pd
import os
import time
import argparse
import pylgmath

import pdb

from rclpy.serialization import deserialize_message, serialize_message
from rosidl_runtime_py.utilities import get_message

from posegraph_utils import *
from torrent.torrent_utils import *

from dataclasses import dataclass, field
from typing import Optional


class LiveReconstructor:
    """
    Maintains a persistent connection to all submap-wise .db3 pieces and polls for new files at a fixed rate

    Source layout (relative to deconstructed_path):
        {UUID}00000.db3
        {UUID}00001.db3
        {UUID}00002.db3
        ...

    Target layout (relative to bag_path):
        vertices/vertices_0.db3
        edges/edges_0.db3
        index/index_0.db3
        data/pointmap/pointmap_0.db3
        data/pointmap_ptr/pointmap_ptr_0.db3
        data/waypoint_name/waypoint_name_0.db3
        data/env_info/env_info_0.db3
    """

    def __init__(self, deconstructed_path: str, metadata_path: str, output_dir: str, poll_hz: float = 1.0, robot_id: int = 0):
        self.deconstructed_path = deconstructed_path
        self.metadata_path = metadata_path
        self.output_dir = output_dir
        self.poll_hz = poll_hz
        self.robot_id = robot_id
        os.makedirs(f'{output_dir}', exist_ok=True)

        # topics we are reading from
        self.tables = ['vtr_index','env_info','waypoint_name','vertices','edges','pointmap','pointmap_ptr']
        self.master_data = {table: [] for table in self.tables}

        # topics we are writing to (+ pointmap_v0)
        self._db_relpaths = {
                'vertices'     : f'{output_dir}/vertices/vertices_0.db3',
                'edges'        : f'{output_dir}/edges/edges_0.db3',
                'index'        : f'{output_dir}/index/index_0.db3',
                'pointmap'     : f'{output_dir}/data/pointmap/pointmap_0.db3',
                'pointmap_v0'  : f'{output_dir}/data/pointmap_v0/pointmap_v0_0.db3',
                'pointmap_ptr' : f'{output_dir}/data/pointmap_ptr/pointmap_ptr_0.db3',
                'waypoint_name': f'{output_dir}/data/waypoint_name/waypoint_name_0.db3',
                'env_info'     : f'{output_dir}/data/env_info/env_info_0.db3'
            }

        self.topics = {
            'vertices'     : 'vtr_pose_graph_msgs/msg/Vertex',
            'edges'        : 'vtr_pose_graph_msgs/msg/Edge',
            'index'        : 'vtr_pose_graph_msgs/msg/Graph',
            'pointmap'     : 'vtr_lidar_msgs/msg/PointMap',
            'pointmap_v0'  : 'vtr_lidar_msgs/msg/PointMap',
            'pointmap_ptr' : 'vtr_lidar_msgs/msg/PointMapPointer',
            'waypoint_name': 'vtr_tactic_msgs/msg/WaypointNames',
            'env_info'     : 'vtr_tactic_msgs/msg/EnvInfo'
        }

        # make directories for VTR
        print(f'[INIT]: self.output_dir: {self.output_dir}')
        for key, topic in self.topics.items():
            if key == 'index': 
                os.makedirs(f'{self.output_dir}/index', exist_ok=True)
            elif key == 'edges' or key == 'vertices': 
                os.makedirs(f'{self.output_dir}/{key}', exist_ok=True)
            else:
                os.makedirs(f'{self.output_dir}/data/{key}', exist_ok=True)

        # initiate sqlite connections and cursors
        self._conns: dict[str, sqlite3.Connection | None] = {k: sqlite3.connect(path) for k, path in self._db_relpaths.items()}
        self._cursors: dict[str, sqlite3.Cursor | None] = {k: self._conns[k].cursor() for k in self._conns}

        # per-source row cursors (last rowid seen)
        self._last_rowid = {k: 0 for k in self._db_relpaths}    
        self._init_database()
        self._index_written = False

        # file tracking
        self.db_files = []
        self.db_written = []

        # list pieces that have been previewed, but not written
        # TODO: modify to be a dict of piece previews (robot-ids)
        self.pieces : list[Piece] = []

    # ============= PUBLIC =================
    def run(self):
        print(f"[LiveReconstructor] polling at {self.poll_hz} Hz  (Ctrl-C to stop)")
        try:
            while True:
                self._poll()
                time.sleep(1.0 / self.poll_hz)
        except KeyboardInterrupt:
            print("\n[LiveReconstructor] stopped.")
        finally:
            self._close()

    # ============= PRIVATE ================
    def _poll(self):
        """
        Read new deconstructed data, metadata
        """
        # get files
        self.metadata_files = sorted(
            [f for f in os.listdir(self.metadata_path) if f.endswith('.torrent')]
        )
        if not self.metadata_files:
            print('[ERROR]: no metadata')
            return
        
        # get topology
        # TODO: handle metadata files that update
        # TODO: this will have to be live instead of written-to .torrents
        for metadata_file in self.metadata_files:
            fname = os.path.join(self.metadata_path, metadata_file)
            self.pieces = self._parse_metadata(fname)

        self.db_files = sorted(
            [f for f in os.listdir(self.deconstructed_path) if f.endswith('.db3')],
            key=lambda x: int(x.split('.')[0], 16)
        )
        if not self.db_files:
            return
        
        # get data from self.db_files
        for db_file in self.db_files:
            if db_file in self.db_written:
                continue

            poll_data = self._parse_piece(db_file)
            self._ingest_piece(poll_data)

            self.db_written.append(db_file)

    def _parse_piece(self, db_file: pd.DataFrame):
        """
        Read from a submap-wise .db3
        """
        poll_data = {}
        conn = sqlite3.connect(os.path.join(self.deconstructed_path, db_file))
        for table in self.tables:
            try:
                df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
                poll_data[table] = df
            except Exception as e:
                print(f"Warning: Could not read table {table} from {db_file}: {e}")
                poll_data[table] = pd.DataFrame() 

        conn.close()
        # preview_piece(poll_data)

        to_id = inspect_ros_data(poll_data['edges'].iloc[-1]).to_id
        from_id = inspect_ros_data(poll_data['edges'].iloc[0]).from_id

        return poll_data
    
    def _ingest_piece(self, poll_data: pd.DataFrame):
        # take poll_data, fill relevant Piece
        first_vid = inspect_ros_data(poll_data['vertices'].iloc[0]).id
        for i, piece in enumerate(self.pieces):
            if first_vid == piece.top_vertices[0].vertex_id:
                print("[INGEST]: match found")
                self.pieces[i].vertices = poll_data['vertices']
                self.pieces[i].edges = poll_data['edges']
                self.pieces[i].pointmap = poll_data['pointmap']
                self.pieces[i].pointmap_v0 = poll_data['pointmap']
                self.pieces[i].pointmap_ptr = poll_data['pointmap_ptr']
                self.pieces[i].waypoint_name = poll_data['waypoint_name']
                self.pieces[i].vtr_index = poll_data['vtr_index']
                self.pieces[i].env_info = poll_data['env_info']
                self._write_message(self.pieces[i])
                self.pieces.pop(i)

    def _parse_metadata(self, metadata_file: str):
        pieces = inspect_torrent(metadata_file)
        # create piece with topology preview
        return pieces
        
    # ============ .db3 INTERFACE ==============
    def _init_database(self):
        """
        initiate .db3s using topics and cursors
        """
        for k, path in self._db_relpaths.items():
            if os.path.exists(path):
                os.remove(path)
            # reconnect to fresh file
            self._conns[k] = sqlite3.connect(path)
            self._cursors[k] = self._conns[k].cursor()
            
            print(f'[INIT_DB]: {k} at {self._conns[k]}, cursor {self._cursors[k]}, topic {self.topics[k]}')
            
            self._cursors[k].execute("""
                CREATE TABLE topics (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    type TEXT NOT NULL,
                    serialization_format TEXT NOT NULL,
                    offered_qos_profiles TEXT
                )
            """)
            self._cursors[k].execute("""
                CREATE TABLE messages (
                    id INTEGER PRIMARY KEY,
                    topic_id INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL,
                    data BLOB NOT NULL
                )
            """)       
    
            # insert topics
            self._cursors[k].execute("""
                INSERT INTO topics (name, type, serialization_format, offered_qos_profiles)
                VALUES (?, ?, ?, ?)
            """, (k, self.topics[k], "cdr", ""))
            self._last_rowid[k] = self._cursors[k].lastrowid
            self._conns[k].commit()
            print(f"[INIT_DB]: Initialized: {self._db_relpaths[k]} | topic_id={self.topics[k]}")

    def _write_message(self, piece: Piece):
        """
        Connect to existing conns and write messages
        """
        print("[WRITE]")
        field_map = {
            'index':       'vtr_index',
            'pointmap_v0': 'pointmap',
        }

        for k in self._cursors:
            if k == 'index' and self._index_written:
                continue
            
            field = field_map.get(k, k)  # use mapped name if exists, else k itself
            df = getattr(piece, field)
            if df is None or df.empty:
                print(f"[WRITE] Skipping '{k}': no data")
                continue

            self._cursors[k].executemany("""
                INSERT INTO messages (topic_id, timestamp, data)
                VALUES (?, ?, ?)
            """,  [
                (self._last_rowid[k], int(row['timestamp']), row['data'])
                for _, row in df.iterrows()
            ])
            self._conns[k].commit()

            if k == 'index':
                self._index_written = True

    # ============= DEBUG ==================
    def _plot_preview(self):
        # TODO:
        print('_plot_preview')

    # ============= CLEANUP ================
    def _close(self):
        for k, conn in self._conns.items():
            if conn:
                conn.commit()
                conn.close()
                self._conns[k] = None
        print("[LiveReconstructor] connections closed.")

# +++++++++++++ HELPERS ++++++++++++++++
def preview_piece(piece):
    edges = piece['edges']
    vertices = piece['vertices']

    to_id = inspect_ros_data(edges.iloc[-1]).to_id

    v0 = inspect_ros_data(vertices.iloc[0]).id
    vf = inspect_ros_data(vertices.iloc[-1]).id

    preview = f"""
        Preview:
        v0: {hex(v0)}, vf: {hex(vf)}, Egress: {hex(to_id)}, 
    """
    print(preview)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Script to deconstruct posegraphs submap-wise')
    parser.add_argument('-b', '--bag_name', default='none', help="The name of the posegraph") # TODO: watch deconstructed dir generally
    parser.add_argument('-m', '--metadata', type=str, default='/home/asrl/ASRL/vtr3/torrent_ws/scripts/torrent/metadata', help="")
    parser.add_argument('-r', '--robot_id', type=int, default=0, help="")
    parser.add_argument('--poll_hz', type=float, default=1.0)
    parser.add_argument('--posegraph_root', default='/home/asrl/ASRL/vtr3/temp/pgs')
    parser.add_argument('--piece_root', default='/home/asrl/ASRL/vtr3/temp/pcs')
    args = parser.parse_args()
    bag_name = args.bag_name
    robot_id = args.robot_id

    output_dir = os.path.join(args.posegraph_root, f"r{bag_name}", 'graph')
    piece_path = os.path.join(args.piece_root, args.bag_name)
    metadata_path = args.metadata

    print(f'[MAIN]: output_dir: {output_dir}')
    rec = LiveReconstructor(
        deconstructed_path=piece_path,
        metadata_path=metadata_path,
        output_dir=output_dir,
        poll_hz=args.poll_hz,
        robot_id=robot_id
    )
    rec.run()