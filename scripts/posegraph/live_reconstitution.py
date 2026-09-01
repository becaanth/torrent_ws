import sqlite3
import pandas as pd
import os
import time
import threading
import argparse
import logging
import bisect # to search the skeleton

from rclpy.serialization import deserialize_message, serialize_message
from rosidl_runtime_py.utilities import get_message

from .posegraph_utils import *
from torrent.torrent_utils import inspect_torrent

from vtr_pose_graph_msgs.msg import Vertex, Edge, EdgeType, EdgeMode
from vtr_pose_graph_msgs.msg import Graph, MapInfo
from vtr_common_msgs.msg import LieGroupTransform

import contextlib
import pdb
import traceback

logger = logging.getLogger(__name__)

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
        self._topo_lock = threading.Lock()
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
        logging.info(f'self.output_dir: {self.output_dir}')
        for key, _ in self.topics.items():
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

        # file tracking
        self.db_files = []
        self.db_written = []

        # list pieces that have been previewed, but not written
        self.pieces : dict[int, Piece] = {}
        self.global_skeleton_rowids = {
            'vertices': {},  # vertex_id (int) -> sqlite rowid
            'edges': {}      # (from_id, to_id) (tuple) -> sqlite rowid
        }

    def run(self):
        logging.info(f"polling at {self.poll_hz} Hz  (Ctrl-C to stop)")
        try:
            while True:
                self._poll()
                time.sleep(1.0 / self.poll_hz)
        except KeyboardInterrupt:
            logging.info("\nstopped.")
        finally:
            self._close()

    def update_topology(self, robot_id, topology):
        logging.info(f"updating topology for robot {robot_id} : topology hash {hash(topology)}")
        with self._topo_lock:
            self.topology[str(robot_id)] = topology

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
        Read new deconstituted data, metadata
        """
        # look at metadata (via torrent callback from peer)
        if self.topology:
            with self._topo_lock:
                topo_items = list(self.topology.items())

            for r_id, topo in topo_items:
                # dont need topology for pieces this robot made
                logging.debug(f"_poll hash of self.topology r_id {r_id}: {hash(topo)}")
                # if local pieces; ignore
                if str(r_id) == str(self.robot_id):
                    continue

                # decode topology from binary
                pieces, idx = inspect_torrent(topo)
                logging.info(f"[DIAG] inspect_torrent(r_id={r_id}) -> {len(pieces)} pieces, idx_present={idx is not None}")

                # if we havent written index before, write index
                if idx is None:
                    logging.info("inspect_torrent: idx is None")
                if not self._index_written and idx is not None: # write MapInfo (from remote)
                    self._write_index(idx)
                    self._index_written = True    

                # iterate through all of the pieces
                for piece in pieces:
                    vid = piece.top_vertices[0].vertex_id # TODO: first vid is not a unique identifier
                    # is this a new piece
                    if vid not in self.pieces:
                        piece.metadata_written = False
                        piece.data_ingested = False
                        self.pieces[vid] = piece
                        logging.info(f"piece vid: {vid}: top_edges{piece.top_edges}")
                    # this a tracked piece
                    else:
                       continue

            # Write metadata skeletons for any piece not yet written
            logging.info(f"self.pieces {self.pieces.keys()}")
            for vid, piece in self.pieces.items():
                if not getattr(piece, 'metadata_written', False):  # empty dict = not yet written
                    self._write_metadata(piece)

        # look at pieces on disk (in pcs), to overwrite skeleton
        # go through local pieces
        if not os.path.exists(self.pieces_path):
            logging.critical('self.pieces_path does not exist')
            return

        # get folders of pieces (N for N robots)
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
                    first_vid = int(db_file[:-4],16)

                    target_piece = None
                    if folder_name != self.robot_id:
                        if first_vid in self.pieces:
                            target_piece = self.pieces[first_vid]
                        else:
                            for vid, piece in self.pieces.items():
                                if piece.top_vertices:
                                    v_min = piece.top_vertices[0].vertex_id
                                    v_max = piece.top_vertices[-1].vertex_id
                                    if v_min <= first_vid <= v_max:
                                        target_piece = piece
                                        break
                        
                        # if remote and not in our metadata, must be from a future session
                        if target_piece is None:
                            continue

                    poll_data = self._parse_piece(folder_name, db_file)
                    # skip incomplete data
                    if not poll_data:
                        continue
                    if poll_data['vertices'].empty:
                        continue
                    if poll_data['edges'].empty:
                        continue
                    if poll_data['pointmap'].empty:
                        continue
                    if poll_data['pointmap_ptr'].empty:
                        continue

                    # if this robot's pieces
                    if folder_name == self.robot_id:
                        # self._ingest_local_piece(poll_data)
                        logging.debug(f"local piece, skip {db_file}")

                    else:
                        if not self.pieces:
                            continue # wait for metadata
                        self._ingest_remote_piece(poll_data, target_piece)
                        logging.debug(f"ingest remote piece {db_file}")
                    
                    logging.info(target_piece)
                    self.db_written.append(db_file)
                except Exception as e:
                    logging.warning(f"{db_file} because {e}")
                    traceback.print_exc()
                    continue

    def _parse_piece(self, folder_path: str, db_file: str):
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
            except Exception as e:
                # logging.debug(f"_parse_piece id | {db_file} | {e}")
                poll_data[table] = pd.DataFrame() 

        conn.close()
        return poll_data
    
    def _ingest_local_piece(self, poll_data: dict):
        """Writes rows directly using standard INSERT statements bypassing skeleton mappings."""
        s = time.time()
        field_map = {'index': 'vtr_index', 'pointmap_v0': 'pointmap'}
        
        for k in self._db_relpaths:
            if k == 'index':
                if not self._index_written: # write MapInfo (local)
                    self._index_written = True   
                    graph_msg = inspect_ros_data(poll_data['vtr_index'].iloc[0])
                    idx = {
                                'set': graph_msg.map_info.set,
                                'root_vid': graph_msg.map_info.root_vid,
                                'lng': graph_msg.map_info.lng,
                                'lat': graph_msg.map_info.lat,
                                'theta': graph_msg.map_info.theta,
                                'scale': graph_msg.map_info.scale,
                                'curr_major_id': graph_msg.curr_major_id,
                                'curr_minor_id': graph_msg.curr_minor_id
                            }
                    self._write_index(idx)
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
        logging.info(f"Direct Local Ingestion Complete: {time.time() - s:.4f}s")

    def _ingest_remote_piece(self, poll_data: pd.DataFrame, piece: Piece):
        # take poll_data, fill relevant Piece
        piece.vertices = poll_data['vertices']
        piece.edges = poll_data['edges']
        piece.pointmap = poll_data['pointmap']
        piece.pointmap_v0 = poll_data['pointmap']
        piece.pointmap_ptr = poll_data['pointmap_ptr']
        piece.waypoint_name = poll_data['waypoint_name']
        piece.vtr_index = poll_data['vtr_index']
        piece.env_info = poll_data['env_info']
        self._write_message(piece)
        piece.data_ingested = True
        logging.info(f"ingest found match for submap {piece.top_vertices[0].vertex_id}")
    
    # ============ .db3 INTERFACE ==============
    def _init_database(self):
        """
        initiate .db3s using topics and cursors (attach if already exists)
        """
        for k, path in self._db_relpaths.items():
            os.makedirs(os.path.dirname(path), exist_ok=True)

            # reconnect to fresh file
            with self._open(k) as conn:
                logging.info(f'init db {k} at {self._conns[k]}, topic {self.topics[k]}')
                existing = {
                    row[0] for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('topics','messages')"
                    ).fetchall()
                }
            
                if {'topics', 'messages'} <= existing:
                    row = conn.execute(
                        "SELECT id FROM topics WHERE name = ?", (k,)
                    ).fetchone()
                    if row is not None:
                        self._last_rowid[k] = row[0]  # reuse existing topic_id
                        logging.debug(f"attached to existing {k} (topic_id={row[0]})")
                        continue
                else:
                    conn.execute("""
                        CREATE TABLE IF NOT EXISTS topics (
                            id INTEGER PRIMARY KEY,
                            name TEXT NOT NULL,
                            type TEXT NOT NULL,
                            serialization_format TEXT NOT NULL,
                            offered_qos_profiles TEXT
                        )
                    """)
                    conn.execute("""
                        CREATE TABLE IF NOT EXISTS messages (
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
        # logging.info(f"_write_metadata for piece {piece}")
        # if not piece.skeleton_rowids:
        #     piece.skeleton_rowids = {'vertices': {}, 'edges': {}}
        rowids = self.global_skeleton_rowids

        new_vertices = [v for v in piece.top_vertices if v.vertex_id not in rowids['vertices']]
        # populate .db3 with topology information    
        with self._open('vertices') as conn:
            conn.execute("BEGIN;")
            for v in new_vertices:
                m_v = Vertex(id=v.vertex_id)
                s_v = serialize_message(m_v)
                cur = conn.execute("""
                        INSERT INTO messages (topic_id, timestamp, data)
                        VALUES (?, ?, ?)
                    """, (self._last_rowid['vertices'], -1, s_v)) # timestamp is -1 default
                rowids['vertices'][v.vertex_id] = cur.lastrowid

            conn.execute("COMMIT;")

        new_edges = [e for e in piece.top_edges if (e.from_id, e.to_id) not in rowids['edges']]  
        with self._open('edges') as conn:
            conn.execute("BEGIN;")
            for e in new_edges:
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
                rowids['edges'][(e.from_id,e.to_id)] = cur.lastrowid
            
            conn.execute("COMMIT;")

        logging.info(f"[DIAG] _write_metadata wrote {len(new_vertices)} new verts, {len(new_edges)} new edges for vid={hex(piece.top_vertices[0].vertex_id)}")
        piece.metadata_written = True
        logging.info(f"_write_metadata: {piece.top_vertices[0].vertex_id}")

    def _write_message(self, piece: Piece):
        """
        Connect to existing conns and write messages
        """
        logging.info(f"_write_message for piece {piece}")

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
                logging.debug(f"skipping '{k}': no data")
                continue
            
            with self._open(k) as conn:
                conn.execute("BEGIN;")
                logging.info(f"opening {k}")                            
                if k in skeleton_keys:
                    rowid_map = self.global_skeleton_rowids[k]
                    updates = []
                    inserts = []

                    for _, row in df.iterrows():
                        ros_row = inspect_ros_data(row)
                        # determine the key
                        if k == 'vertices':
                            row_key = int(ros_row.id)
                        elif k == 'edges':
                            row_key = (int(ros_row.from_id), int(ros_row.to_id))
                        rid = rowid_map.get(row_key)
                        if rid is None:
                            logging.warning(
                                f"no skeleton rowid for {k} key={row_key} in this piece "
                                f"(vid={hex(piece.top_vertices[0].vertex_id)}) -- inserting fresh row "
                                f"instead of dropping data"
                            )
                            inserts.append((row['data'], int(row['timestamp']), rid))  
                            continue
                        updates.append((row['data'], int(row['timestamp']), rid))  

                    # Apply all updates to existing skeleton rows
                    if updates:
                        conn.executemany("UPDATE messages SET data = ?, timestamp = ? WHERE id = ?", updates)

                    if inserts:
                        for row_key, timestamp, data in inserts:
                            cur = conn.execute("""
                                INSERT INTO messages (topic_id, timestamp, data)
                                VALUES (?, ?, ?)
                            """, (self._last_rowid[k], timestamp, data))
                            rowid_map[row_key] = cur.lastrowid

                else:
                    conn.executemany("""
                        INSERT INTO messages (topic_id, timestamp, data)
                        VALUES (?, ?, ?)
                    """,  [
                        (self._last_rowid[k], int(row['timestamp']), row['data'])
                        for _, row in df.iterrows()
                    ])

                conn.execute("COMMIT;")
                logging.info(f"_write_message: {piece.top_vertices[0].vertex_id}")
                print(f"_write_message: {piece.top_vertices[0].vertex_id}")

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
        logging.info("connections closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Script to reconstitute posegraphs submap-wise')
    parser.add_argument('-p', '--posegraph', default='none', help="The name of the posegraph") 
    parser.add_argument('--poll_hz', type=float, default=1.0)
    parser.add_argument('--posegraph_root', default='/home/asrl/ASRL/vtr3/temp/pgs')
    parser.add_argument('--piece_root', default='/home/asrl/ASRL/vtr3/temp/pcs')
    args = parser.parse_args()
    posegraph = args.posegraph
    robot_id = os.getenv("ROBOT_ID")

    output_dir = os.path.join(args.posegraph_root, f"{posegraph}", 'graph')
    piece_path = os.path.join(args.piece_root, args.posegraph)

    logging.info(f'output_dir: {output_dir}')
    print(f'output_dir: {output_dir}')
    rec = Reconstitutor(
        pieces_path=piece_path,
        robot_id=robot_id,
        output_dir=output_dir,
        poll_hz=args.poll_hz,
    )
    rec.run()