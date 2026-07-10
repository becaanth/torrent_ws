import sqlite3
import pandas as pd
import os
import time
import argparse

from rclpy.serialization import deserialize_message, serialize_message
from rosidl_runtime_py.utilities import get_message

from .posegraph_utils import *
from torrent.torrent_utils import inspect_torrent

from vtr_pose_graph_msgs.msg import Vertex, Edge, EdgeType, EdgeMode
from vtr_pose_graph_msgs.msg import Graph, MapInfo
from vtr_common_msgs.msg import LieGroupTransform

import contextlib
import pdb

class Reconstitutor:
    """
    Maintains a persistent connection to all submap-wise .db3 pieces and polls for new files at a fixed rate

    Source layout (relative to pieces_path):
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

    def __init__(self, pieces_path: str, output_dir: str, robot_id, poll_hz: float = 1.0):
        self.robot_id = robot_id
        self.pieces_path = pieces_path
        self.output_dir = output_dir
        self.poll_hz = poll_hz
        os.makedirs(f'{self.output_dir}', exist_ok=True)

        # topics we are reading from
        self.tables = ['vtr_index','env_info','waypoint_name','vertices','edges','pointmap','pointmap_ptr']
        self.master_data = {table: [] for table in self.tables}
        self.topology = {}

        # topics we are writing to (+ pointmap_v0)
        self._db_relpaths = {
                'vertices'     : f'{self.output_dir}/vertices/vertices_0.db3',
                'edges'        : f'{self.output_dir}/edges/edges_0.db3',
                'index'        : f'{self.output_dir}/index/index_0.db3',
                'pointmap'     : f'{self.output_dir}/data/pointmap/pointmap_0.db3',
                'pointmap_v0'  : f'{self.output_dir}/data/pointmap_v0/pointmap_v0_0.db3',
                'pointmap_ptr' : f'{self.output_dir}/data/pointmap_ptr/pointmap_ptr_0.db3',
                'waypoint_name': f'{self.output_dir}/data/waypoint_name/waypoint_name_0.db3',
                'env_info'     : f'{self.output_dir}/data/env_info/env_info_0.db3'
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
        print(f'[Reconstitutor]: self.output_dir: {self.output_dir}')
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
        print(f"[Reconstitutor]: polling at {self.poll_hz} Hz  (Ctrl-C to stop)")
        try:
            while True:
                self._poll()
                time.sleep(1.0 / self.poll_hz)
        except KeyboardInterrupt:
            print("\n[Reconstitutor]: stopped.")
        finally:
            self._close()

    def update_topology(self, robot_id, topology):
        print(f"[Reconstitutor]: updating topology for robot {robot_id}")
        self.topology[str(robot_id)] = topology

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
        # TODO: refactor =================
        if self.topology:
            existing = {p.top_vertices[0].vertex_id: p for p in self.pieces}
            new_pieces = []
            for r_id, topo in self.topology.items():
                # dont need topology for pieces this robot made
                if r_id == self.robot_id:
                    continue
                
                pieces, idx = inspect_torrent(topo)
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
                    # time.sleep(0.01) # ANTHONY - delay for dev
                    self._write_metadata(piece)

        # go through local chunks
        if not os.path.exists(self.pieces_path):
            return
        

        robot_subfolders = [f.path for f in os.scandir(self.pieces_path) if f.is_dir()]
        for robot_subfolder in robot_subfolders:
            folder_name = os.path.basename(robot_subfolder)

            db_files=sorted(
                [f for f in os.listdir(robot_subfolder) if f.endswith('.db3')],
                key=lambda x: int(x.split('.')[0], 16)
            )
            
            for db_file in db_files:
                if db_file in self.db_written:
                    continue

                try:
                    poll_data = self._parse_piece(folder_name, db_file)

                    # if THIS robot's pieces
                    if folder_name == self.robot_id:
                        self._ingest_local_piece(poll_data)
                        print(f"[Reconstitutor]: ingest local piece {db_file}")

                    else:
                        if not self.pieces:
                            continue # wait for metadata
                        self._ingest_remote_piece(poll_data)
                        print(f"[Reconstitutor]: ingest remote piece {db_file}")

                    self.db_written.append(db_file)
                except:
                    print(f"[Reconstitutor]: WARNING Could not read from {db_file}")
                    continue

    def _parse_piece(self, folder_path: str, db_file: pd.DataFrame):
        """
        Read from a submap-wise .db3
        """

        poll_data = {}
        db_path = os.path.join(self.pieces_path, folder_path, db_file)
        conn = sqlite3.connect(db_path, isolation_level=None)

        for table in self.tables:
            try:
                df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
                poll_data[table] = df
            except:
                print(f"[Reconstitutor]: _parse_piece exception")
                poll_data[table] = pd.DataFrame() 

        conn.close()
        return poll_data
    
    def _ingest_local_piece(self, poll_data: dict):
        """Writes rows directly using standard INSERT statements bypassing skeleton mappings."""
        s = time.time()
        field_map = {'index': 'vtr_index', 'pointmap_v0': 'pointmap'}
        
        for k in self._db_relpaths:
            if k == 'index' and not self._index_written:
                try:
                    self._write_index(poll_data.get('index'))
                except:
                    pass
                continue
            
            field = field_map.get(k, k)
            df = poll_data.get(field)
            if df is None or df.empty:
                continue
                
            with self._open(k) as conn:
                conn.execute("BEGIN;")
                conn.executemany("""
                    INSERT INTO messages (topic_id, timestamp, data)
                    VALUES (?, ?, ?)
                """, [
                    (self._last_rowid[k], int(row['timestamp']), row['data'])
                    for _, row in df.iterrows()
                ])
                conn.execute("COMMIT;")
        print(f"[Reconstitutor]: Direct Local Ingestion Complete: {time.time() - s:.4f}s")

    def _ingest_remote_piece(self, poll_data: pd.DataFrame):
        # take poll_data, fill relevant Piece
        if poll_data['vertices'].empty:
            return
        

        first_vid = inspect_ros_data(poll_data['vertices'].iloc[0]).id
        for i, piece in enumerate(self.pieces):
            if first_vid == piece.top_vertices[0].vertex_id:
                print("[Reconstitutor]: ingest found match")
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
                print(f'[Reconstitutor]: init db {k} at {self._conns[k]}, topic {self.topics[k]}')
            
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
        print(f"[Reconstitutor]: _write_metadata: {time.time() - s}")


    def _write_message(self, piece: Piece):
        """
        Connect to existing conns and write messages
        """
        s = time.time()
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
                print(f"[Reconstitutor]: skipping '{k}': no data")
                continue
            
            with self._open(k) as conn:
                conn.execute("BEGIN;")
                if k in skeleton_keys:
                    rowids = piece.skeleton_rowids.get(k)
                    if not rowids:
                        conn.execute("ROLLBACK;")
                        continue
                    if len(rowids) != len(df):
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
                print(f"[Reconstitutor]: _write_message: {time.time() - s}")

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

    # ============= CLEANUP ================
    def _close(self):
        # nothing to flush, each write closes its own connections
        print("[Reconstitutor]: connections closed.")

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
    parser.add_argument('-p', '--posegraph', default='none', help="The name of the posegraph") # TODO: watch deconstructed dir generally
    parser.add_argument('--poll_hz', type=float, default=1.0)
    parser.add_argument('--posegraph_root', default='/home/asrl/ASRL/vtr3/temp/pgs')
    parser.add_argument('--piece_root', default='/home/asrl/ASRL/vtr3/temp/pcs')
    args = parser.parse_args()
    posegraph = args.posegraph
    robot_id = os.getenv("ROBOT_ID")

    output_dir = os.path.join(args.posegraph_root, f"r{posegraph}", 'graph')
    piece_path = os.path.join(args.piece_root, args.posegraph)

    print(f'[Reconstitutor]: output_dir: {output_dir}')
    rec = Reconstitutor(
        pieces_path=piece_path,
        robot_id=robot_id,
        output_dir=output_dir,
        poll_hz=args.poll_hz,
    )
    rec.run()