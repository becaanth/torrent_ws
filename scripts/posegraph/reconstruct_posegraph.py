import sqlite3
import pandas as pd
import numpy as np
import os
import yaml
import pdb
import argparse

from rclpy.serialization import deserialize_message, serialize_message
from rosidl_runtime_py.utilities import get_message

parser = argparse.ArgumentParser(description = 'Script to deconstruct posegraphs submap-wise')
parser.add_argument('-b', '--bag_name', default='none', help="The name of the posegraph") 
parser.add_argument('-a', '--agent_num', type=int, default=0, help="")
args = parser.parse_args()
bag_name = args.bag_name
agent = args.agent_num

# folder_path = '/home/asrl/ASRL/vtr3/torrent_ws'
folder_path = '/home/asrl/ASRL/vtr3'
chunk_name =  f'torrent_ws/deconstructed/{agent}/' + bag_name
chunks_path = f'{folder_path}/{chunk_name}'

"""
    CHECKLIST:
    - how to deal with disconnected posegraphs
"""

def inspect_ros_data(frame):
    msg = deserialize_message(frame.data, get_message(frame["topic_type"]))
    return msg

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


if __name__ == '__main__':
    # Get and sort .db3 files by hex vertex ID
    db_files = sorted(
        [f for f in os.listdir(chunks_path) if f.endswith('.db3')],
        key=lambda x: int(x.split('.')[0], 16)
    )
    if not db_files:
        print("No .db3 files found — nothing to reconstruct.")
        exit(0)
    # Tables to recover
    tables = ['vtr_index','env_info','waypoint_name','vertices','edges','pointmap','pointmap_ptr']

    master_data = {table: [] for table in tables}

    for db_file in db_files:
        conn = sqlite3.connect(os.path.join(chunks_path, db_file))
        for table in tables:
            try:
                df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
                master_data[table].append(df)
            except Exception as e:
                print(f"Warning: Could not read table {table} from {db_file}: {e}")
        conn.close()

    # Concatenate all segments into one table per topic
    all_data = {}
    for table, dfs in master_data.items():
        if dfs:
            all_data[table] = pd.concat(dfs, ignore_index=True)

    print(all_data['vertices'].head())
    print(all_data['edges'].head())

    output_dir = f'{folder_path}/temp/r{bag_name}'
    print(f'reconstructing to {output_dir}')
    os.makedirs(output_dir, exist_ok=True)

    for key in all_data.keys():
        print(f'serializing: {key}')
        write_rosbag_from_df(all_data[key], output_dir, 0, False)

    print('done writing')