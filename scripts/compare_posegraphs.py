import sqlite3
import pandas as pd
import numpy as np
import os
import pdb
import matplotlib.pyplot as plt
import argparse

from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message
from deconstruct_posegraph import get_db3_elements
from reconstruct_posegraph import inspect_ros_data

parser = argparse.ArgumentParser(prog = 'Plot Point Clouds Path',
                        description = 'Plots point clouds')
parser.add_argument('-b', '--bag_name', default='none', help="The filepath to the pose graph folder. (Usually /a/path/graph)")      # option that takes a value
args = parser.parse_args()
bag_name = args.bag_name

topics = ['vertices','edges','index','pointmap','pointmap_ptr','waypoint_name','env_info']

posegraph_original, posegraph_reconstructed = {},{}
original_dir = '/home/asrl/ASRL/vtr3/temp'
reconstructed_dir = '/home/asrl/ASRL/vtr3/torrent_ws/reconstructed/0'
original_path = f'{original_dir}/{bag_name}/graph'
reconstructed_path = f'{reconstructed_dir}/{bag_name}/{bag_name}_0/graph'

for topic in topics:
    posegraph_original[topic] = get_db3_elements(original_path, topic)
    posegraph_reconstructed[topic] = get_db3_elements(reconstructed_path, topic)

# check vertices
num_vertices = len(posegraph_original['vertices']['df'])
bool_vertex = []
v_o, v_r = [], []
for i in range(num_vertices):
    target_vertex = i
    for j in range(num_vertices):
        if inspect_ros_data(posegraph_original['vertices']['df'].loc[j]).id ==target_vertex:
            vertex_a = posegraph_original['vertices']['df'].loc[j]
            break
    # for j in range(num_vertices):
    #     if inspect_ros_data(posegraph_reconstructed['vertices']['df'].loc[j]).id ==target_vertex:
    #         vertex_b = posegraph_reconstructed['vertices']['df'].loc[j]
    #         break
    vertex_b = posegraph_reconstructed['vertices']['df'].loc[i]
    
    v_o.append(inspect_ros_data(posegraph_original['vertices']['df'].loc[i]).id)
    v_r.append(inspect_ros_data(posegraph_reconstructed['vertices']['df'].loc[i]).id)
    print('a: ', v_o[i], '| b: ', v_r[i])
    bool_vertex.append(vertex_a.all() == vertex_b.all())

# check edges
num_edges = len(posegraph_original['edges']['df'])
bool_edge = []
for i in range(num_edges):
    target_edge = i
    for j in range(num_edges):
        if inspect_ros_data(posegraph_original['edges']['df'].loc[j]).from_id ==target_edge:
            edge_a = posegraph_original['edges']['df'].loc[j]
            break
    # for j in range(num_edges):
    #     if inspect_ros_data(posegraph_reconstructed['edges']['df'].loc[j]).from_id ==target_edge:
    #         edge_b = posegraph_reconstructed['edges']['df'].loc[j]
    #         break
    edge_b = posegraph_reconstructed['edges']['df'].loc[i]
    bool_edge.append(edge_a.all() == edge_b.all())

bool_index = posegraph_original['index']['df'].equals(posegraph_reconstructed['index']['df'])
bool_waypoint_name = posegraph_original['waypoint_name']['df'].equals(posegraph_reconstructed['waypoint_name']['df'])
bool_env_info = posegraph_original['env_info']['df'].equals(posegraph_reconstructed['env_info']['df'])
bool_pointmap = posegraph_original['pointmap']['df'].equals(posegraph_reconstructed['pointmap']['df'])
bool_pointmap_ptr = posegraph_original['pointmap_ptr']['df'].equals(posegraph_reconstructed['pointmap_ptr']['df'])

print('vertices: ', bool_vertex)
print('edges:    ', bool_edge)
print('index:    ', bool_index)
print('env_info:    ', bool_env_info)
print('waypoint_name:    ', bool_waypoint_name)
print('pointmap:    ', bool_pointmap)
print('pointmap_ptr:    ', bool_pointmap_ptr)

pdb.set_trace()

plt.plot(range(len(v_o)), v_o, label='original')
plt.plot(range(len(v_r)), v_r, label='reconstructed')
plt.xlabel('index in .db3')
plt.ylabel('vertex id')
plt.legend()
plt.show()