import sqlite3
import pandas as pd
import numpy as np
import os
import pdb

from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message

folder_path = '/home/asrl/ASRL/vtr3/temp'
bag_name = 're_baseline'
bag_path = f'{folder_path}/{bag_name}/graph'

"""
    CHECKLIST:
    - index
    x vertices
    x edges
    x pointmap
    x pointmap_v0, copy pointmap on reconstruction
    x pointmap_ptr (we can sidestep this)
    x waypoint_name
    x env_info
"""

def get_teach_vertices(bag_path):
    which_data = 'vertices'
    conn = sqlite3.connect(f'{bag_path}/{which_data}/{which_data}_0.db3')

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

    # GET TEACH VERTICES
    v_ids = np.array(())
    for i in range(len(full_df)):
        msg = deserialize_message(full_df.loc[i].data, get_message(full_df.loc[i]["topic_type"]))
        v_ids = np.append(v_ids, msg.id)

    teach_vertices_df = full_df[v_ids < 1e6]
    conn.close()
    return teach_vertices_df, v_ids[v_ids < 1e6]

def get_teach_edges(bag_path):
    # GET TEACH EDGES
    which_data = 'edges'
    conn = sqlite3.connect(f'{bag_path}/{which_data}/{which_data}_0.db3')

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

    to_ids = np.array(())
    from_ids = np.array(())
    e_ids = np.array(())
    for i in range(len(full_df)):
        msg = deserialize_message(full_df.loc[i].data, get_message(full_df.loc[i]["topic_type"]))
        if msg.mode.mode == 1: # taken in manual mode
            e_ids = np.append(e_ids, i)
            to_ids = np.append(to_ids, msg._to_id)
            from_ids = np.append(from_ids, msg._from_id)

    teach_edges_df = full_df.loc[e_ids]
    conn.close()

    return teach_edges_df, to_ids, from_ids

def get_teach_index(bag_path):
    which_data = 'index'
    conn = sqlite3.connect(f'{bag_path}/{which_data}/{which_data}_0.db3')

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

    conn.close()
    return full_df

def get_teach_submaps(bag_path):
    # GET TEACH SUBMAPS
    which_data = 'pointmap'
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

    s_ids = np.array(())
    for i in range(len(full_df)):
        msg = deserialize_message(full_df.loc[i].data, get_message(full_df.loc[i]["topic_type"]))
        s_ids = np.append(s_ids, msg.vertex_id)

    teach_submaps_df = full_df[s_ids < 1e6]
    conn.close()
    return teach_submaps_df, s_ids[s_ids < 1e6]

def get_teach_submap_ptrs(bag_path):
    # GET TEACH SUBMAPS
    which_data = 'pointmap_ptr'
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

    this_vids = np.array(())
    map_vids = np.array(())
    for i in range(len(full_df)):
        msg = deserialize_message(full_df.loc[i].data, get_message(full_df.loc[i]["topic_type"]))
        this_vids = np.append(this_vids, msg.this_vid)
        map_vids = np.append(map_vids, msg.map_vid)

    submap_ptrs_df = full_df[(this_vids + map_vids) < 1e6]
    conn.close()
    return submap_ptrs_df, map_vids[map_vids < 1e6]

def set_env_info(bag_path, num_teach_vtx):
    wp = pd.DataFrame({
    "topic_name": ["env_info"],
    "topic_type": ["vtr_tactic_msgs/msg/EnvInfo"],
    "timestamp": [-1],
    "data": [b'\x00\x01\x00\x00\x01']
    }, index=[0])

    waypoints_df = pd.concat([wp] * num_teach_vtx, ignore_index=True)
    return waypoints_df

def set_teach_waypoints(bag_path, num_teach_vtx):
    wp = pd.DataFrame({
    "topic_name": ["waypoint_name"],
    "topic_type": ["vtr_tactic_msgs/msg/WaypointNames"],
    "timestamp": [-1],
    "data": [b'\x00\x01\x00\x00\x01\x00\x00\x00\x00']
    }, index=[0])

    waypoints_df = pd.concat([wp] * num_teach_vtx, ignore_index=True)
    return waypoints_df


# get vertices, edges, submaps
tv, vids = get_teach_vertices(bag_path)
te, to_ids, from_ids = get_teach_edges(bag_path)
ti = get_teach_index(bag_path)
submaps, sids = get_teach_submaps(bag_path)
submap_ptrs, map_vids = get_teach_submap_ptrs(bag_path)
waypoints = set_teach_waypoints(bag_path, len(tv))
env_info = set_env_info(bag_path, len(tv))

output_dir = bag_name + '_deconstructed'
os.makedirs(output_dir, exist_ok=True)
print(output_dir)
# write to .db3 vertex chunks
for i, sid in enumerate(sids):
    # write a .db3 for each sid
    sid = int(sid)
    chunk_submap = submaps.loc[[i]] # keep as a df not a Series object
    # map_ids are stored indexwise, find idxs where map_id = sid
    idxs = np.where(map_vids == sid)[0]
    chunk_submap_ptrs = submap_ptrs.loc[idxs]
    chunk_waypoints = waypoints.loc[idxs]
    chunk_env_info = env_info.loc[idxs]
    
    # get vertices at these idxs
    v_mask = np.isin(vids, idxs)
    valid_vtx = np.where(v_mask)[0]
    # sort them in ascending order and track idxs
    sort_vidx = np.argsort(vids[v_mask])
    chunk_vtxs = tv.loc[valid_vtx[sort_vidx]]

    # get corresponding edges
    e_mask = np.isin(from_ids, idxs)
    valid_edges = np.where(e_mask)[0]
    # sort them in ascending order and track idxs
    sort_eidx = np.argsort(from_ids[e_mask])
    chunk_edges = te.loc[valid_edges[sort_eidx]]

    # convert df to sql to write to chunkwise db3
    conn = sqlite3.connect(f'{output_dir}/{i}.db3')
    ti.to_sql('vtr_index', conn, if_exists='replace', index=False) # index is same for all chunks
    chunk_vtxs.to_sql('vertices', conn, if_exists='replace', index=False)
    chunk_edges.to_sql('edges', conn, if_exists='replace', index=False)
    chunk_env_info.to_sql('env_info', conn, if_exists='replace', index=False)
    chunk_waypoints.to_sql('waypoint_name', conn, if_exists='replace', index=False)
    chunk_submap.to_sql('pointmap', conn, if_exists='replace', index=False)
    chunk_submap_ptrs.to_sql('pointmap_ptr', conn, if_exists='replace', index=False)
    conn.close()

print(f'done deconstructing {bag_name}')