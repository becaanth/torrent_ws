import sqlite3
import os
import pandas as pd
from rclpy.serialization import deserialize_message, serialize_message
from posegraph_utils import *
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
    parser.add_argument('-p', '--posegraph', required=True,help="Bag name (subdirectory under folder_path)")
    parser.add_argument('-c', '--chunk', required=True,help="Bag name (subdirectory under folder_path)")
    args = parser.parse_args()

    # Path to your .db3 file
    temp = os.getenv("VTRTEMP")
    rid = os.getenv("ROBOT_ID")
    pg = args.posegraph
    chunk = args.chunk
    chunk_str = f"0x{chunk}"
    chunk_rid = extract_robot_id(int(chunk_str, 16))

    db_path = f"{temp}/pcs/{pg}_{rid}/{chunk_rid}/{chunk}.db3"
    print(db_path)
    poll_data = {}
    conn = sqlite3.connect(db_path, isolation_level=None)
    tables = ['vtr_index','env_info','waypoint_name','vertices','edges','pointmap','pointmap_ptr']
    for table in tables:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
            poll_data[table] = df
        except Exception as e:
            poll_data[table] = pd.DataFrame() 
    conn.close()

    edges = []
    for _ , e in poll_data['edges'].iterrows():
        edge = inspect_ros_data(e)
        print(f"edge: mode {edge.mode.mode},type {edge.type.type}, to {edge.to_id}, from {edge.from_id}")
        edges.append(edge)

    