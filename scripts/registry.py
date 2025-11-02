import os
import yaml

import pdb

folder_path = '/home/asrl/ASRL/vtr3/torrent_ws'
bag_name = 're_baseline'
chunk_name =  bag_name + '_deconstructed'
chunks_path = f'{folder_path}/{chunk_name}'

# List all .db3 chunk files in sorted order
db_files = [f for f in os.listdir(chunks_path) if f.endswith('.db3')]
chunks = sorted([int(f.split('.')[0]) for f in db_files])

pdb.set_trace()