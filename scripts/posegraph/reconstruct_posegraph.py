import sqlite3
import pandas as pd
import numpy as np
import os
import yaml
import pdb
import argparse

from rclpy.serialization import deserialize_message, serialize_message
from rosidl_runtime_py.utilities import get_message

from posegraph_utils import *
"""
    CHECKLIST:
    - how to deal with disconnected posegraphs
"""

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = 'Script to deconstruct posegraphs submap-wise')
    parser.add_argument('-b', '--bag_name', default='none', help="The name of the posegraph") 
    parser.add_argument('-a', '--agent_num', type=int, default=0, help="")
    args = parser.parse_args()
    bag_name = args.bag_name
    agent = args.agent_num

    # folder_path = '/hodb_fileme/asrl/ASRL/vtr3/torrent_ws'
    folder_path = '/home/asrl/ASRL/vtr3'
    chunk_name =  f'torrent_ws/deconstructed/{agent}/' + bag_name
    chunks_path = f'{folder_path}/{chunk_name}'

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
        # print(f'serializing: {key}')
        # s = 'topic_type'
        # n = 'topic_name'
        # print(f'{all_data[key][n].iloc[0]} : {all_data[key][s].iloc[0]}')
        write_rosbag_from_df(all_data[key], output_dir, 0, False)

    print('done writing')