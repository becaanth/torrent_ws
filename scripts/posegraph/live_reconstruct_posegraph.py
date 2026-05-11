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
