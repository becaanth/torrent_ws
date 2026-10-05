import sqlite3
import os
import pandas as pd
from rclpy.serialization import deserialize_message, serialize_message
from posegraph_utils import *
import matplotlib.pyplot as plt
from collections import defaultdict, deque
import argparse
import pdb

def find_graph_components(from_list, to_list):
    # Handle empty graph edge case
    if not from_list or not to_list:
        return {"is_connected": False, "total_components": 0, "components": []}
        
    # 1. Build Adjacency List & Collect Unique Nodes
    # Using a set here automatically eliminates duplicate edge entries
    graph = defaultdict(set)
    unvisited_nodes = set()
    
    for u, v in zip(from_list, to_list):
        graph[u].add(v)
        graph[v].add(u)
        unvisited_nodes.add(u)
        unvisited_nodes.add(v)
        
    components = []
    
    # 2. Iterate until every single node has been visited
    while unvisited_nodes:
        # Pick any random unvisited node to start a new component search
        start_node = next(iter(unvisited_nodes))
        
        # Initialize BFS for this specific component
        current_component = set()
        queue = deque([start_node])
        unvisited_nodes.remove(start_node)
        current_component.add(start_node)
        
        while queue:
            current = queue.popleft()
            for neighbor in graph[current]:
                # If neighbor is in unvisited_nodes, it hasn't been seen yet
                if neighbor in unvisited_nodes:
                    unvisited_nodes.remove(neighbor)
                    current_component.add(neighbor)
                    queue.append(neighbor)
                    
        # Save the finished isolated cluster
        components.append(list(current_component))
        
    # 3. Compile the structural breakdown
    return {
        "is_connected": len(components) == 1,
        "total_components": len(components),
        "components": components
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Teach, Torrent, Repeat Agent")
    parser.add_argument('-p', '--posegraph', required=True,help="Posegraph name")
    parser.add_argument('-d', '--db', required=True,help="topic name", default='edges')
    args = parser.parse_args()

    # Path to your .db3 file
    temp = os.getenv("VTRTEMP")
    pg = args.posegraph
    db = args.db
    if db == 'vertices' or db == 'edges':
        db_path = f"{temp}/pgs/{pg}/graph/{db}/{db}_0.db3"
    else:
        db_path = f"{temp}/pgs/{pg}/graph/data/{db}/{db}_0.db3"
    print(db_path)
    poll_data = {}

    conn = sqlite3.connect(db_path, isolation_level=None)
    for table in ["topics", "messages"]:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
            poll_data[table] = df
        except Exception as e:
            # logging.debug(f"_parse_piece id | {db_file} | {e}")
            poll_data[table] = pd.DataFrame() 

    conn.close()

    if db == 'edges':
        to_list = []
        from_list = []
        for _, row in poll_data['messages'].iterrows():
            msg_type = get_message(poll_data["topics"]['type'].iloc[0])
            msg_data = row['data']
            msg = deserialize_message(msg_data, msg_type)
            to_list.append(msg.to_id)
            from_list.append(msg.from_id)
            print(f"mode: {msg.mode.mode} type: {msg.type.type} to: {msg.to_id}, from: {msg.from_id}")

        to_set = set(to_list)
        from_set = set(from_list)

        analysis = find_graph_components(from_list, to_list)
        print(f"Is fully connected?: {analysis['is_connected']}")
        print(f"Total isolated groups: {analysis['total_components']}")
        print(f"Groups breakdown: {analysis['components']}")

        for component in analysis['components']:
            print(f"len {len(component)}")
    elif db == 'pointmap_ptr':
        ptrs = []
        for _, row in poll_data['messages'].iterrows():
            msg_type = get_message(poll_data["topics"]['type'].iloc[0])
            msg_data = row['data']
            msg = deserialize_message(msg_data, msg_type)
            ptrs.append((msg.map_vid, msg.this_vid))
            print(f"map_vid {msg.map_vid}, this_vid {msg.this_vid}")

    elif db == 'pointmap':
        pm = []
        for _, row in poll_data['messages'].iterrows():
            msg_type = get_message(poll_data["topics"]['type'].iloc[0])
            msg_data = row['data']
            msg = deserialize_message(msg_data, msg_type)
            pm.append((msg.vertex_id))
            print(f"pointmap vertex id {msg.vertex_id}")