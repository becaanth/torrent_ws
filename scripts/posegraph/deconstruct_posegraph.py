import sqlite3
import pandas as pd
import numpy as np
import os
import argparse
import argparse
import time
import pdb

from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message

parser = argparse.ArgumentParser(description = 'Script to deconstruct posegraphs submap-wise')
parser.add_argument('-b', '--bag_name', default='none', help="The name of the posegraph") 
parser.add_argument('-a', '--agent_num', type=int, default=0, help="")
args = parser.parse_args()
bag_name = args.bag_name
agent = args.agent_num

# folder_path = '/home/asrl/ASRL/vtr3/temp/nanook/posegraph'
folder_path = '/home/asrl/ASRL/vtr3/temp'
bag_path = f'{folder_path}/{bag_name}/graph'
deconstructed_path = f'/home/asrl/ASRL/vtr3/torrent_ws/deconstructed/{agent}/'

"""
    - index
    - vertices
    - edges
    - pointmap
    - pointmap_v0, copy pointmap on reconstruction
    - pointmap_ptr
    - waypoint_name
    - env_info
"""

def inspect_ros_data(frame):
    msg = deserialize_message(frame.data, get_message(frame["topic_type"]))
    return msg

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


def pad_file_to_exact_size(path, target_size):
    # IMPORTANT: stat AFTER sqlite connection is closed
    current_size = os.path.getsize(path)

    if current_size > target_size:
        raise RuntimeError(
            f"{path} too large: {current_size} > {target_size}"
        )

    missing = target_size - current_size
    if missing == 0:
        return

    with open(path, "ab") as f:
        f.write(b"\x00" * missing)

    # sanity check
    final_size = os.path.getsize(path)
    assert final_size == target_size, (
        f"padding failed: {final_size} != {target_size}"
    )

if __name__ == '__main__':
    vertices = get_db3_elements(bag_path, 'vertices')
    edges = get_db3_elements(bag_path, 'edges')
    index = get_db3_elements(bag_path, 'index')
    pointmap = get_db3_elements(bag_path, 'pointmap')
    pointmap_ptr = get_db3_elements(bag_path, 'pointmap_ptr')
    env_info = get_db3_elements(bag_path, 'env_info')
    waypoint_name = get_db3_elements(bag_path, 'waypoint_name')

    output_dir = deconstructed_path + bag_name
    os.makedirs(f"{output_dir}", exist_ok=True)
    print(f'Output Directory: {output_dir}')

    # write to .db3 vertex chunks
    for i, sid in enumerate(pointmap['submap_ids']):
        data_db_path = f'{output_dir}/{str(hex(int(sid)))[2:].zfill(16)}.db3'
        chunk_submap = pointmap['df'].loc[[i]]

        # find rows in pointmap_ptr belonging to this submap, get their vertex IDs
        ptr_row_idxs = np.where(pointmap_ptr['map_ids'] == sid)[0]
        relevant_vids = pointmap_ptr['this_vids'][ptr_row_idxs]
        chunk_submap_ptrs = pointmap_ptr['df'].loc[ptr_row_idxs]
        chunk_waypoints = waypoint_name['df'].loc[ptr_row_idxs]
        chunk_env_info = env_info['df'].loc[ptr_row_idxs]

        # get vertices whose ID is in relevant_vids
        v_mask = np.isin(vertices['vertex_ids'], relevant_vids)
        valid_vtx = np.where(v_mask)[0]
        sort_vidx = np.argsort(vertices['vertex_ids'][v_mask])
        chunk_vtxs = vertices['df'].loc[valid_vtx[sort_vidx]]

        # get edges whose from_id is in relevant_vids
        e_mask = np.isin(edges['from_ids'], relevant_vids)
        valid_edges = np.where(e_mask)[0]
        sort_eidx = np.argsort(edges['from_ids'][e_mask])
        chunk_edges = edges['df'].loc[valid_edges[sort_eidx]]

        # WRITE TO .db3s
        # ========================================================
        # DATA
        # convert df to sql to write to chunkwise db3
        conn = sqlite3.connect(data_db_path)
        index['df'].to_sql('vtr_index', conn, if_exists='replace', index=False) # index is same for all chunks
        chunk_vtxs.to_sql('vertices', conn, if_exists='replace', index=False)
        chunk_edges.to_sql('edges', conn, if_exists='replace', index=False)
        chunk_env_info.to_sql('env_info', conn, if_exists='replace', index=False)
        chunk_waypoints.to_sql('waypoint_name', conn, if_exists='replace', index=False)
        chunk_submap.to_sql('pointmap', conn, if_exists='replace', index=False)
        chunk_submap_ptrs.to_sql('pointmap_ptr', conn, if_exists='replace', index=False)
        conn.close()

        # pad to piece_size
        PIECE_SIZE = 2 * 1024 * 1024 # 2 MiB - 4 byte SQL overhead
        pad_file_to_exact_size(data_db_path, PIECE_SIZE)

        # time.sleep(0.1)


    print(f'done deconstructing {bag_name}')