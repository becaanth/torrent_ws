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

from vtr_pose_graph_msgs.msg import Vertex, Edge, EdgeType, EdgeMode
from vtr_pose_graph_msgs.msg import Graph, MapInfo
from vtr_common_msgs.msg import LieGroupTransform

from dataclasses import dataclass, field
from typing import Optional
import contextlib

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
        self._conns: dict[str, sqlite3.Connection | None] = {k: sqlite3.connect(path, isolation_level=None) for k, path in self._db_relpaths.items()}

        # per-source row cursors (last rowid seen)
        self._last_rowid = {k: 0 for k in self._db_relpaths}    
        self._init_database()
        self._index_written = False
        self._metadata_written = False # TODO: be more clever

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
    @contextlib.contextmanager
    def _open(self, key):
        """Short-lived connection, guaranteed to close even on error."""
        conn = sqlite3.connect(self._db_relpaths[key], isolation_level=None, timeout=5.0)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA busy_timeout=5000;")
            yield conn
        finally:
            conn.close()

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

        # Rebuild pieces from metadata, preserving existing skeleton_rowids
        existing = {p.top_vertices[0].vertex_id: p for p in self.pieces}
        new_pieces = []
        for metadata_file in self.metadata_files:
            fname = os.path.join(self.metadata_path, metadata_file)
            pieces, idx = self._parse_metadata(fname)
            if not self._index_written: # write MapInfo
                self._write_index(idx)
                self._index_written = True            

            for piece in pieces:
                vid = piece.top_vertices[0].vertex_id
                if vid in existing:
                    new_pieces.append(existing[vid])  # preserve rowids
                else:
                    new_pieces.append(piece)
        self.pieces = new_pieces
      
        # Write metadata skeletons for any piece not yet written
        for piece in self.pieces:
            if not piece.metadata_written:  # empty dict = not yet written
                time.sleep(0.01) # ANTHONY - delay for dev
                self._write_metadata(piece)
        
        # TODO: handle metadata files that update
        # TODO: this will have to be live instead of written-to .torrents
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
            time.sleep(0.1) # ANTHONY - delay for dev

            self.db_written.append(db_file)

    def _parse_piece(self, db_file: pd.DataFrame):
        """
        Read from a submap-wise .db3
        """
        poll_data = {}
        conn = sqlite3.connect(os.path.join(self.deconstructed_path, db_file), isolation_level=None)

        for table in self.tables:
            try:
                df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
                poll_data[table] = df
            except:
                print(f"Warning: Could not read table {table} from {db_file}")
                poll_data[table] = pd.DataFrame() 

        conn.close()

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
                self.pieces[i].metadata_written = True

    def _parse_metadata(self, metadata_file: str):
        pieces, idx = inspect_torrent(metadata_file)
        # create piece with topology preview
        return pieces, idx
        
    # ============ .db3 INTERFACE ==============
    def _init_database(self):
        """
        initiate .db3s using topics and cursors
        """
        for k, path in self._db_relpaths.items():
            if os.path.exists(path):
                os.remove(path)
            for sidecar in (path + "-wal", path + "-shm"):
                if os.path.exists(sidecar):
                    os.remove(sidecar)

            # reconnect to fresh file
            with self._open(k) as conn:
                print(f'[INIT_DB]: {k} at {self._conns[k]}, topic {self.topics[k]}')
            
                conn.execute("""
                    CREATE TABLE topics (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        type TEXT NOT NULL,
                        serialization_format TEXT NOT NULL,
                        offered_qos_profiles TEXT
                    )
                """)
                conn.execute("""
                    CREATE TABLE messages (
                        id INTEGER PRIMARY KEY,
                        topic_id INTEGER NOT NULL,
                        timestamp INTEGER NOT NULL,
                        data BLOB NOT NULL
                    )
                """)       
    
                # insert topics
                cur = conn.execute("""
                    INSERT INTO topics (name, type, serialization_format, offered_qos_profiles)
                    VALUES (?, ?, ?, ?)
                """, (k, self.topics[k], "cdr", ""))
                self._last_rowid[k] = cur.lastrowid
                print(f"[INIT_DB]: Initialized: {self._db_relpaths[k]} | topic_id={self.topics[k]}")

    def _write_metadata(self, piece: Piece):
        """
        Write skeleton rows for vertices and edges using topology from top_vertices/top_edges.
        data is NULL until the real piece arrives.
        """
        s = time.time()
        rowids = {'vertices':[], 'edges':[]}
        # populate ROS2 messages with topology information    
        with self._open('vertices') as conn:
            conn.execute("BEGIN;")
            for v in piece.top_vertices:
                m_v = Vertex(id=v.vertex_id)
                s_v = serialize_message(m_v)
                cur = conn.execute("""
                        INSERT INTO messages (topic_id, timestamp, data)
                        VALUES (?, ?, ?)
                    """, (self._last_rowid['vertices'], -1, s_v)) # timestamp is -1 default
                rowids['vertices'].append(cur.lastrowid)

            conn.execute("COMMIT;")
            
        with self._open('edges') as conn:
            conn.execute("BEGIN;")
            for e in piece.top_edges:
                tf = LieGroupTransform(xi = e.xi, cov_set=False)
                edge_mode = EdgeMode()
                edge_mode.mode = EdgeMode.UNKNOWN
                edge_type = EdgeType()
                edge_type.type = e.type
                m_e = Edge(type=edge_type, mode=edge_mode, from_id=e.from_id, to_id=e.to_id, t_to_from=tf)
                s_e = serialize_message(m_e)
                cur = conn.execute("""
                    INSERT INTO messages (topic_id, timestamp, data)
                    VALUES (?, ?, ?)
                """, (self._last_rowid['edges'], -1, s_e)) # timestamp is -1 default
                rowids['edges'].append(cur.lastrowid)
            
            conn.execute("COMMIT;")

        piece.skeleton_rowids = rowids
        piece.metadata_written = True
        print(f"_write_metadata: {time.time() - s}")


    def _write_message(self, piece: Piece):
        """
        Connect to existing conns and write messages
        """
        s = time.time()
        print("[WRITE]")
        field_map = {
            'index':       'vtr_index',
            'pointmap_v0': 'pointmap',
        }
        skeleton_keys = {'vertices', 'edges'}
        for k in self._db_relpaths:
            if k == 'index':
                continue
            
            field = field_map.get(k, k)  # use mapped name if exists, else k itself
            df = getattr(piece, field)
            if df is None or df.empty:
                print(f"[WRITE] Skipping '{k}': no data")
                continue
            
            with self._open(k) as conn:
                conn.execute("BEGIN;")
                if k in skeleton_keys:
                    rowids = piece.skeleton_rowids.get(k)
                    if not rowids:
                        print(f"[WRITE] ERROR: no skeleton rowids for {k} - skipping to avoid duplicates")
                        conn.execute("ROLLBACK;")
                        continue
                    if len(rowids) != len(df):
                        print(f"[WRITE] ERROR: skeleton/data length mismatch for '{k}': "
                        f"{len(rowids)} skeleton rows vs {len(df)} data rows — skipping")
                        conn.execute("ROLLBACK;")
                        continue
                    # Fill in the NULL skeletons in order
                    conn.executemany("""
                        UPDATE messages SET data = ?, timestamp = ?
                        WHERE id = ?
                        """, [
                            (row['data'], int(row['timestamp']), rowid) 
                            for rowid, (_, row) in zip(rowids, df.iterrows())
                        ])
                else:           
                    # no skeleton exists                                  
                    conn.executemany("""
                        INSERT INTO messages (topic_id, timestamp, data)
                        VALUES (?, ?, ?)
                    """,  [
                        (self._last_rowid[k], int(row['timestamp']), row['data'])
                        for _, row in df.iterrows()
                    ])
                
                conn.execute("COMMIT;")

            print(f"_write_message: {time.time() - s}")

    def _write_index(self, index_msg):
        # write index.db3, containing MapInfo message
        map_info = MapInfo(
            set=index_msg['set'],
            root_vid=index_msg['root_vid'],
            lng=index_msg['lng'],
            lat=index_msg['lat'],
            theta=index_msg['theta'],
            scale=index_msg['scale']
        )
        graph = Graph(
            curr_major_id=index_msg['curr_major_id'],
            curr_minor_id=index_msg['curr_minor_id'],
            map_info=map_info
        )

        with self._open('index') as conn:
            conn.execute("BEGIN;")
            s_graph = serialize_message(graph)
            cur = conn.execute("""
                INSERT INTO messages (topic_id, timestamp, data)
                VALUES (?, ?, ?)
            """, (1, -1, s_graph)) # timestamp is -1 default            
            conn.execute("COMMIT;")


    # ============= DEBUG ==================
    def _plot_preview(self):
        # TODO:
        print('_plot_preview')

    # ============= CLEANUP ================
    def _close(self):
        # nothing to flush, each write closes its own connections
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