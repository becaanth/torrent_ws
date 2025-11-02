import sqlite3
import pandas as pd
import numpy as np
import os
import yaml
import pdb

from rclpy.serialization import deserialize_message, serialize_message
from rosidl_runtime_py.utilities import get_message

folder_path = '/home/asrl/ASRL/vtr3/torrent_ws'
bag_name = 're_baseline'
chunk_name =  'deconstructed/' + bag_name
chunks_path = f'{folder_path}/{chunk_name}'

"""
    CHECKLIST:
    - index
    x vertices
    x edges
    x pointmap
    - pointmap_v0
    x pointmap_ptr (we can sidestep this)
    x waypoint_name
    x env_info
"""

def write_metadata_yaml(df, bag_dir, topic_name, topic_type):
    """
    Generate metadata.yaml for a single-topic ROS 2 bag.
    
    Args:
        df (pd.DataFrame): DataFrame with columns ['topic_name', 'topic_type', 'timestamp', 'data']
        bag_dir (str): directory to save metadata.yaml
        topic_name (str): ROS topic name (matches df['topic_name'].iloc[0])
    """
    if topic_name == 'index':
        df = df.loc[[0]] # we only need one message
    starting_ts = int(df['timestamp'].min())
    duration_ns = int(df['timestamp'].max() - starting_ts)
    num_messages = len(df)

    metadata = {
        "rosbag2_bagfile_information": {
            "version": 4,
            "storage_identifier": "sqlite3",
            "relative_file_paths": [f"{topic_name.strip('/').replace('/', '_')}_0.db3"],
            "duration": {"nanoseconds": duration_ns},
            "starting_time": {"nanoseconds_since_epoch": starting_ts},
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
            "compression_format": "",
            "compression_mode": ""
        }
    }

    yaml_path = os.path.join(f'{bag_dir}', "metadata.yaml")
    with open(yaml_path, 'w') as f:
        yaml.dump(metadata, f, sort_keys=False)

    print(f"✅ metadata.yaml written to {yaml_path}")

def write_rosbag(df, bag_path, topic_name, topic_type):
    if topic_name == 'index':
        df = df.loc[[0]] # we only need one message
    db_path = f'{bag_path}/{topic_name}_0.db3'
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

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
    print(f"✅ Wrote ROS2 bag for topic {topic_name}: {db_path}")

def write_rosbag_from_df(df, output_dir):
    """
    Write a ROS2-compatible .db3 bag from a DataFrame with columns:
    ['topic_name', 'topic_type', 'timestamp', 'data']
    """
    topic_name = df['topic_name'].iloc[0]
    topic_type = df['topic_type'].iloc[0]

    if topic_name == 'vtr_index':
        topic_name = 'index'

    if topic_name == 'vertices' or topic_name == 'edges' or topic_name == 'index':
        bag_path = f'{output_dir}/graph/{topic_name}'
        os.makedirs(f'{bag_path}', exist_ok=True)
        
    else:
        bag_path = f'{output_dir}/graph/data/{topic_name}'
        os.makedirs(f'{bag_path}', exist_ok=True)

    if topic_name == 'pointmap':
        write_metadata_yaml(df, bag_path, topic_name, topic_type)
        write_rosbag(df, bag_path, topic_name, topic_type)
        # do the same for pointmap_v0
        topic_name = 'pointmap_v0'
        bag_path = f'{output_dir}/graph/data/{topic_name}'
        os.makedirs(f'{bag_path}', exist_ok=True)
        write_metadata_yaml(df, bag_path, topic_name, topic_type)
        write_rosbag(df, bag_path, topic_name, topic_type)
    else:
        write_metadata_yaml(df, bag_path, topic_name, topic_type)
        write_rosbag(df, bag_path, topic_name, topic_type)

# List all .db3 chunk files in sorted order
db_files = sorted([f for f in os.listdir(chunks_path) if f.endswith('.db3')])

# Tables to recover
tables = [
    'vtr_index',
    'env_info',
    'waypoint_name',
    'vertices',
    'edges',
    'pointmap',
    # 'pointmap_v0',
    'pointmap_ptr'
]

# Dictionary to hold chunked tables
chunked_data = {table: [] for table in tables}

# Loop over each chunk DB
for i, db_file in enumerate(db_files):
    conn = sqlite3.connect(os.path.join(chunks_path, db_file))
    for table in tables:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
            chunked_data[table].append(df)
        except Exception as e:
            print(f"Warning: Could not read table {table} from {db_file}: {e}")
    
    conn.close()

# Concatenate chunks and sort by timestamp where applicable
all_data = {}
for table, dfs in chunked_data.items():
    if dfs:  # make sure there is at least one chunk
        concatenated = pd.concat(dfs, ignore_index=True)
        all_data[table] = concatenated

# Now all_data['vertices'], all_data['edges'], etc. contain fully concatenated tables
print(all_data['vertices'].head())
print(all_data['edges'].head())

output_dir = 'reconstructed/' + bag_name
os.makedirs(output_dir, exist_ok=True)

for key in all_data.keys():
    print(f'serializing: {key}')
    write_rosbag_from_df(all_data[key], output_dir)


print('done writing')