import libtorrent as lt
import pprint
import numpy as np
import msgpack
import argparse

from dataclasses import dataclass, field
from typing import Optional

from rclpy.serialization import deserialize_message, serialize_message
from rosidl_runtime_py.utilities import get_message

from torrent_utils import *
from scripts.posegraph.posegraph_utils import *

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = 'Seed mutable torrents')
    parser.add_argument('-p', '--posegraph', type=str, default=None, help="Name of posegraph") 
    args = parser.parse_args()
  
    posegraph = args.posegraph
    path = f'/home/asrl/ASRL/vtr3/torrent_ws/scripts/torrent/metadata/{posegraph}.torrent'
    inspect_torrent(path)