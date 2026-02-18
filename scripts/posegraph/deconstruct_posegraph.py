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

parser = argparse.ArgumentParser(prog = 'Plot Point Clouds Path',
                        description = 'Plots point clouds')
parser.add_argument('-b', '--bag_name', default='none', help="The filepath to the pose graph folder. (Usually /a/path/graph)")      # option that takes a value
args = parser.parse_args()
bag_name = args.bag_name

folder_path = '/home/asrl/ASRL/vtr3/temp'
agent = f"{0}"
deconstructed_path = f'/home/asrl/ASRL/vtr3/torrent_ws/deconstructed/{agent}/'
bag_path = f'{folder_path}/{bag_name}/graph'

"""
    - index
    - vertices
    - edges
    - pointmap
    x pointmap_v0, copy pointmap on reconstruction
    - pointmap_ptr
    - waypoint_name
    - env_info
"""

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
        # get teach vertices
        v_ids = np.array(())
        for i in range(len(full_df)):
            msg = deserialize_message(full_df.loc[i].data, get_message(full_df.loc[i]["topic_type"]))
            v_ids = np.append(v_ids, msg.id)

        teach_vertices_df = full_df[v_ids < 1e6]
        res = {
            'df': teach_vertices_df, 
            'vertex_ids' : v_ids[v_ids < 1e6]
        }
    
    elif which_data == 'edges':
            to_ids, from_ids, e_ids = np.array(()), np.array(()), np.array(())
            for i in range(len(full_df)):
                msg = deserialize_message(full_df.loc[i].data, get_message(full_df.loc[i]["topic_type"]))
                if msg.mode.mode == 1: # taken in manual mode
                    e_ids = np.append(e_ids, i)
                    to_ids = np.append(to_ids, msg._to_id)
                    from_ids = np.append(from_ids, msg._from_id)
            teach_edges_df = full_df.loc[e_ids]
            res = {
                'df' : teach_edges_df, 
                'to_ids' : to_ids, 
                'from_ids': from_ids
            }
    
    elif which_data == 'pointmap':
        s_ids = np.array(())
        for i in range(len(full_df)):
            msg = deserialize_message(full_df.loc[i].data, get_message(full_df.loc[i]["topic_type"]))
            s_ids = np.append(s_ids, msg.vertex_id)
        teach_submaps_df = full_df[s_ids < 1e6]
        res = {
            'df': teach_submaps_df, 
            'submap_ids' : s_ids[s_ids < 1e6]
        }

    elif which_data == 'pointmap_ptr':
        this_vids, map_vids = np.array(()), np.array(())
        for i in range(len(full_df)):
            msg = deserialize_message(full_df.loc[i].data, get_message(full_df.loc[i]["topic_type"]))
            this_vids = np.append(this_vids, msg.this_vid)
            map_vids = np.append(map_vids, msg.map_vid)

        submap_ptrs_df = full_df[(this_vids + map_vids) < 1e6]
        res = {
            'df' : submap_ptrs_df,
            'map_ids' : map_vids[map_vids < 1e6]
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
    os.makedirs(output_dir, exist_ok=True)
    print(f'Output Directory: {output_dir}')

    # write to .db3 vertex chunks
    for i, sid in enumerate(pointmap['submap_ids']):
        db_path = f'{output_dir}/{i}.db3'
        # write a .db3 for each sid
        sid = int(sid)
        chunk_submap = pointmap['df'].loc[[i]] # keep as a df not a Series object
        # map_ids are stored indexwise, find idxs where map_id = sid
        idxs = np.where(pointmap_ptr['map_ids'] == sid)[0]
        chunk_submap_ptrs = pointmap_ptr['df'].loc[idxs]
        chunk_waypoints = waypoint_name['df'].loc[idxs]
        chunk_env_info = env_info['df'].loc[idxs]
        
        # get vertices at these idxs
        v_mask = np.isin(vertices['vertex_ids'], idxs)
        valid_vtx = np.where(v_mask)[0]
        # sort them in ascending order and track idxs
        sort_vidx = np.argsort(vertices['vertex_ids'][v_mask])
        chunk_vtxs = vertices['df'].loc[valid_vtx[sort_vidx]]

        # get corresponding edges
        e_mask = np.isin(edges['from_ids'], idxs)
        valid_edges = np.where(e_mask)[0]
        # sort them in ascending order and track idxs
        sort_eidx = np.argsort(edges['from_ids'][e_mask])
        chunk_edges = edges['df'].loc[valid_edges[sort_eidx]]

        # convert df to sql to write to chunkwise db3
        conn = sqlite3.connect(db_path)
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
        pad_file_to_exact_size(db_path, PIECE_SIZE)
        # time.sleep(1)

    print(f'done deconstructing {bag_name}')