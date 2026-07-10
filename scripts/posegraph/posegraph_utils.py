import sqlite3
import pandas as pd
import numpy as np
import os
import yaml
from dataclasses import dataclass, field
from typing import Optional

from rclpy.serialization import deserialize_message, serialize_message
from rosidl_runtime_py.utilities import get_message

# general utils
def inspect_ros_data(frame):
    msg = deserialize_message(frame.data, get_message(frame["topic_type"]))
    return msg

def pad_file_to_exact_size(path, target_size):
    # IMPORTANT: start AFTER sqlite connection is closed
    current_size = os.path.getsize(path)

    if current_size > target_size:
        raise RuntimeError(
            f"{path} too large: {current_size} > {target_size}"
        )

    missing = target_size - current_size
    if missing == 0:
        print("map size == padding")
        return

    with open(path, "ab") as f:
        f.write(b"\x00" * missing)

    # sanity check
    final_size = os.path.getsize(path)
    assert final_size == target_size, (
        f"padding failed: {final_size} != {target_size}"
    )


# getters
def get_db3_elements(bag_path, which_data):
    """
    Generic read .db3 files to replace bespoke functions
    """
    if which_data in ['vertices', 'edges', 'index']:
        conn = sqlite3.connect(f'{bag_path}/{which_data}/{which_data}_0.db3')
    else:
        conn = sqlite3.connect(f'{bag_path}/data/{which_data}/{which_data}_0.db3')

    # Merge messages with topic info
    full_df = pd.read_sql_query("""
    SELECT 
        t.name AS topic_name,
        t.type AS topic_type,
        m.timestamp,
        m.data
    FROM messages AS m
    JOIN topics AS t ON m.topic_id = t.id
    ORDER BY m.timestamp;
    """, conn)

    if which_data == 'vertices':
        v_ids = np.array((), dtype=np.uint64)
        for i in range(len(full_df)):
            msg = deserialize_message(full_df.loc[i].data, get_message(full_df.loc[i]["topic_type"]))
            v_ids = np.append(v_ids, np.uint64(msg.id))

        res = {
            'df': full_df,
            'vertex_ids': v_ids
        }

    elif which_data == 'edges':
        to_ids = np.array((), dtype=np.uint64)
        from_ids = np.array((), dtype=np.uint64)
        e_ids = np.array((), dtype=np.uint64)
        for i in range(len(full_df)):
            msg = deserialize_message(full_df.loc[i].data, get_message(full_df.loc[i]["topic_type"]))
            if msg.mode.mode == 1: # taken in manual mode
                e_ids = np.append(e_ids, np.uint64(i))
                to_ids = np.append(to_ids, np.uint64(msg._to_id))
                from_ids = np.append(from_ids, np.uint64(msg._from_id))
        res = {
            'df': full_df.loc[e_ids],
            'to_ids': to_ids,
            'from_ids': from_ids
        }

    elif which_data == 'pointmap':
        s_ids = np.array((), dtype=np.uint64)
        for i in range(len(full_df)):
            msg = deserialize_message(full_df.loc[i].data, get_message(full_df.loc[i]["topic_type"]))
            s_ids = np.append(s_ids, np.uint64(msg.vertex_id))
        res = {
            'df': full_df,
            'submap_ids': s_ids
        }

    elif which_data == 'pointmap_ptr':
        this_vids = np.array((), dtype=np.uint64)
        map_vids = np.array((), dtype=np.uint64)
        for i in range(len(full_df)):
            msg = deserialize_message(full_df.loc[i].data, get_message(full_df.loc[i]["topic_type"]))
            this_vids = np.append(this_vids, np.uint64(msg.this_vid))
            map_vids = np.append(map_vids, np.uint64(msg.map_vid))
        res = {
            'df': full_df,
            'this_vids': this_vids,
            'map_ids': map_vids
        }

    elif which_data in ['index', 'env_info', 'waypoint_name']:
        res = {
            'df' : full_df
        }
    
    else:
        print('[deconstruct_posegraph]: invalid topic requested')

    conn.close()
    return res


# writers
def write_metadata_yaml(df, bag_dir, topic_name, topic_type, segment_num, partial):
    """
    Generate metadata.yaml for a single-topic ROS 2 bag.
    
    Args:
        df (pd.DataFrame): DataFrame with columns ['topic_name', 'topic_type', 'timestamp', 'data']
        bag_dir (str): directory to save metadata.yaml
        topic_name (str): ROS topic name (matches df['topic_name'].iloc[0])
    """
    if topic_name == 'index':
        df = df.loc[[0]] # we only need one message

    # handle discontinuities
    if partial and topic_name == 'edges':
        if segment_num == 0: 
            # remove the last edge
            df = df.iloc[:-1]
        else: # remove first and last edge
            df = df.iloc[1:-1]

    num_messages = len(df)
    if topic_name in ['index', 'vertices', 'edges', 'env_info']:
        duration_nanoseconds = 1
        nanoseconds_since_epoch = 0
    else:
        nanoseconds_since_epoch = int(df['timestamp'].min())
        duration_nanoseconds = int(df['timestamp'].max() - nanoseconds_since_epoch)

    print(f's: {nanoseconds_since_epoch}, d: {duration_nanoseconds}, #: {num_messages}')

    metadata = {
        "rosbag2_bagfile_information": {
            "version": 4,
            "storage_identifier": "sqlite3",
            "relative_file_paths": [f"{topic_name.strip('/').replace('/', '_')}_0.db3"],
            "duration": {"nanoseconds": duration_nanoseconds},
            "starting_time": {"nanoseconds_since_epoch": nanoseconds_since_epoch},
            "message_count": num_messages,
            "topics_with_message_count": [
                {
                "topic_metadata": {
                    "name": topic_name,
                    "type": topic_type,
                    "serialization_format": "cdr",
                    "offered_qos_profiles": ""
                },
                "message_count": num_messages
                }
            ],
            "compression_format": '',
            "compression_mode": ''
        }
    }

    yaml_path = os.path.join(f'{bag_dir}', "metadata.yaml")
    with open(yaml_path, 'w') as f:
        yaml.dump(metadata, f, sort_keys=False)

    print(f"metadata.yaml written to {yaml_path}")

def write_rosbag(df, bag_path, topic_name, topic_type, segment_num, partial):
    if topic_name == 'index':
        df = df.loc[[0]] # we only need one message
    db_path = f'{bag_path}/{topic_name}_0.db3'
    if os.path.exists(db_path):
        os.remove(db_path)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # handle discontinuities
    if partial and topic_name == 'edges':
        if segment_num == 0: 
            # remove the last edge
            df = df.iloc[:-1]
        else: # remove first and last edge
            df = df.iloc[1:-1]

    # --- Create ROS2 bag tables ---
    cur.execute("""
        CREATE TABLE topics (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            type TEXT NOT NULL,
            serialization_format TEXT NOT NULL,
            offered_qos_profiles TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            topic_id INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            data BLOB NOT NULL
        )
    """)

    # --- Insert topic ---
    cur.execute("""
        INSERT INTO topics (name, type, serialization_format, offered_qos_profiles)
        VALUES (?, ?, ?, ?)
    """, (topic_name, topic_type, "cdr", ""))
    topic_id = cur.lastrowid

    # --- Insert messages ---
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO messages (topic_id, timestamp, data)
            VALUES (?, ?, ?)
        """, (topic_id, int(row['timestamp']), row['data']))

    conn.commit()
    conn.close()
    print(f"Wrote ROS2 bag for topic {topic_name}: {db_path}")

def write_rosbag_from_df(df, output_dir, segment_num, partial):
    """
    Write a ROS2-compatible .db3 bag from a DataFrame with columns:
    ['topic_name', 'topic_type', 'timestamp', 'data']
    """
    topic_name = df['topic_name'].iloc[0]
    topic_type = df['topic_type'].iloc[0]

    # make necessary directories
    if topic_name == 'vtr_index':
        topic_name = 'index'
    if topic_name == 'vertices' or topic_name == 'edges' or topic_name == 'index':
        bag_path = f'{output_dir}/graph/{topic_name}'
        os.makedirs(f'{bag_path}', exist_ok=True)
    else:
        bag_path = f'{output_dir}/graph/data/{topic_name}'
        os.makedirs(f'{bag_path}', exist_ok=True)

    # populate .db3s
    if topic_name == 'pointmap':
        # special case
        write_metadata_yaml(df, bag_path, topic_name, topic_type, segment_num, partial)
        write_rosbag(df, bag_path, topic_name, topic_type, segment_num, partial)
        # do the same for pointmap_v0
        topic_name = 'pointmap_v0'
        bag_path = f'{output_dir}/graph/data/{topic_name}'
        os.makedirs(f'{bag_path}', exist_ok=True)
        write_metadata_yaml(df, bag_path, topic_name, topic_type, segment_num, partial)
        write_rosbag(df, bag_path, topic_name, topic_type, segment_num, partial)
    else:
        write_metadata_yaml(df, bag_path, topic_name, topic_type, segment_num, partial)
        write_rosbag(df, bag_path, topic_name, topic_type, segment_num, partial)

# ================ TORRENTS ========================

@dataclass
class Vertex:
    vertex_id: int

@dataclass
class Edge:
    from_id: int
    to_id: int
    mode: int
    type: int
    xi: Optional[np.ndarray] = None
    cov: Optional[np.ndarray] = None

    def __repr__(self):
        return f"Edge({self.from_id}) -> ({self.to_id})"
 
@dataclass
class Piece:
    top_vertices: list[Vertex]
    top_edges: list[Edge]
    metadata_written: bool
    skeleton_rowids: dict = field(default_factory=dict)
    vertices: Optional[pd.DataFrame] = None
    edges: Optional[pd.DataFrame] = None
    pointmap: Optional[pd.DataFrame] = None
    pointmap_v0: Optional[pd.DataFrame] = None
    env_info: Optional[pd.DataFrame] = None
    waypoint_name: Optional[pd.DataFrame] = None
    vtr_index: Optional[pd.DataFrame] = None
    pointmap_ptr: Optional[pd.DataFrame] = None

def parse_chunk(db_path: str) -> tuple[list[Vertex], list[Edge]]:
    """
    Parse a single deconstructed .db3 chunk.
    Returns (vertices, edges) extracted from that chunk.
    """
    conn = sqlite3.connect(db_path)

    vertices: list[Vertex] = []
    edges: list[Edge] = []

    # --- vertices -----------------------------------------------------------
    try:
        vtx_df = pd.read_sql_query(
            "SELECT topic_name, topic_type, timestamp, data FROM vertices", conn
        )
        for _, row in vtx_df.iterrows():
            try:
                msg = deserialize_message(row["data"], get_message(row["topic_type"]))
                vertices.append(Vertex(vertex_id=int(msg.id)))
            except Exception as exc:
                print(f"[parse_chunk] vertex deserialize error in {db_path}: {exc}")
    except Exception as exc:
        print(f"[parse_chunk] no vertices table in {db_path}: {exc}")

    # --- edges --------------------------------------------------------------
    try:
        edge_df = pd.read_sql_query(
            "SELECT topic_name, topic_type, timestamp, data FROM edges", conn
        )
        for _, row in edge_df.iterrows():
            try:
                msg = deserialize_message(row["data"], get_message(row["topic_type"]))
                # Only teach-mode edges (mode == 1)
                if msg.mode.mode != 1:
                    continue
                xi = np.array(msg.t_to_from.xi)
                cov = np.array(msg.t_to_from.cov).reshape(6, 6)
                edges.append(Edge(from_id=int(msg.from_id), to_id=int(msg.to_id), mode=int(msg.mode.mode), type=int(msg.type.type),  xi=xi)) # ignore cov, cov=cov))
            except Exception as exc:
                print(f"[parse_chunk] edge deserialize error in {db_path}: {exc}")
    except Exception as exc:
        print(f"[parse_chunk] no edges table in {db_path}: {exc}")

    conn.close()
    return vertices, edges

def get_map_info(db_path: str):
    conn = sqlite3.connect(db_path)
    idx_df = pd.read_sql_query("SELECT topic_name, topic_type, timestamp, data FROM vtr_index", conn)
    idx_msg = deserialize_message(idx_df.iloc[0]["data"], get_message(idx_df.iloc[0]["topic_type"]))
    return idx_msg

def vertex_to_dict(v: Vertex) -> dict:
    return {"id": v.vertex_id}

def edge_to_dict(e: Edge) -> dict:
    return {
        "from": e.from_id,
        "to":   e.to_id,
        "mode": e.mode,
        "type": e.type,
        "xi":   e.xi.tolist()  if e.xi  is not None else None,
        # "cov":  e.cov.tolist() if e.cov is not None else None, ignore cov
    }

def idx_to_dict(index_msg) -> dict:
    return {
        "curr_major_id": index_msg._curr_major_id,
        "curr_minor_id": index_msg._curr_minor_id,
        "set": index_msg._map_info.set,
        "root_vid": index_msg._map_info.root_vid,
        "lng": index_msg._map_info.lng,
        "lat": index_msg._map_info.lat,
        "theta": index_msg._map_info.theta,
        "scale":index_msg._map_info.scale 
    }

# ============= GRAPH UTILS ============
def extract_robot_id(node_id: int) -> int:
    return (node_id >> 60) & 0xF

def extract_major_id(node_id: int) -> int:
    return (node_id >> 16) & 0xFFFFFFFFFFF

def extract_minor_id(node_id: int) -> int:
    return (node_id) & 0xFFFF

def is_root(node_id: int) -> bool:
    return extract_minor_id(node_id) == 0
