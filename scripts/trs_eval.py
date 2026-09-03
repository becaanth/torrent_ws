import pandas as pd
import matplotlib.pyplot as plt
import glob
import argparse
import os
import colorsys
import matplotlib.colors as mcolors

import pdb

ID_COLOURS = {
  0  : "#3cb44b",
  1  : "#911eb4",
  2  : "#ffe119",
  3  : "#4363d8",
  4  : "#a9a9a9",
  5  : "#f58231",
  6  : "#bfef45",
  7  : "#42d4f4",
  8  : "#f032e6",
  9  : "#fabebe",
  10 : "#ffd8b1", 
  11 : "#fffac8", 
  12 : "#aaffc3", 
  13 : "#000000", 
  14 : "#ffffff", 
  15 : "#e6194b", 
}
DEFAULT_COLOUR='gray'

def get_shades(hex_color):
    rgb = mcolors.to_rgb(hex_color)
    h, l, s = colorsys.rgb_to_hls(*rgb)
    return {
        'base':       hex_color,                                                   # Down Speed (Mbps)
        'darker_20':  mcolors.to_hex(colorsys.hls_to_rgb(h, max(0.0, l * 0.8), s)), # Up Speed (Mbps)
        'darker_40':  mcolors.to_hex(colorsys.hls_to_rgb(h, max(0.0, l * 0.6), s)), # Down Total (MB)
        'lighter_20': mcolors.to_hex(colorsys.hls_to_rgb(h, min(1.0, l * 1.2), s)), # Up Total (MB)
    }

def plot_csv(file_name : str, ID_COLOURS):
    df = pd.read_csv(file_name)
    df['time'] = pd.to_numeric(df['timestamp'].index) - pd.to_numeric(df['timestamp'].index).min()
    df['down_mbps'] = (df['down_payload_rate'] * 8) / 1e6
    df['down_mb'] = (df['down_all_time']) / 1e6
    df['up_mbps'] = (df['up_payload_rate'] * 8) / 1e6
    df['up_mb'] = (df['up_all_time']) / 1e6

    robot_id = df['robot_id'].iloc[0] if 'robot_id' in df.columns else os.path.basename(path)
    base_colour = ID_COLOURS.get(robot_id, 'gray')
    colours = get_shades(base_colour)

def get_seeder_dfs(seeder_paths):
    seeders = {}
    for s in seeder_paths:
        print(s)
        df = pd.read_csv(s)
        try:
            robot_id = df['robot_id'].iloc[0]
        except:
            continue
        seeders[str(robot_id)] = df
    return seeders

def get_info_hashes(seeders):
    info_hashes = {}
    for rid in seeders.keys():
        info_hashes[rid] = set(seeders[rid]['info_hash'].values)
    return info_hashes

def get_peer_dfs(peer_paths):
    peers = {}
    for p in peer_paths:
        print(p)
        df = pd.read_csv(p)
        robot_id = df['robot_id'].iloc[0]
        peers[str(robot_id)] = df
    return peers

def get_session_dfs(seeders, info_hashes, peers):
    """
    metrics for the mutable torrent sesssion 
    key : (seeder, peer metrics for this session)
    """
    sessions = {}
    sids = seeders.keys()
    pids = peers.keys()
    print(info_hashes)

    # get peer info for all info_hashes in rid sesssion
    for sid in sids: # seeders
        sid_accumulated_data = []
        for pid in pids: # peers
            if sid == pid:
                continue

            print(f"s: {sid} p: {pid}")

            peer_info_hashes = set(peers[pid]['info_hash'].values)
            # the set of ihs this peer has heard from this seeder
            peer_session_hashes = peer_info_hashes & info_hashes[sid]
            print(peer_session_hashes)

            peer_data = peers[pid][peers[pid]['info_hash'].isin(peer_session_hashes)]
            if not(peer_data.empty):
                # add to the session stats
                sid_accumulated_data.append(peer_data)

        if sid_accumulated_data:
            peer_for_sid = pd.concat(sid_accumulated_data, ignore_index=True)
        else:
            peer_for_sid = pd.DataFrame()

        sessions[sid] = {"seeder_data" : seeders[sid], "peer_data" : peer_for_sid}
    return sessions



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Teach, Torrent, Repeat Agent")
    parser.add_argument('-p', '--posegraph', type=str, default = 'None')
    parser.add_argument('-r', '--policy', type=str, default = 'rarest_random')
    args = parser.parse_args()

    root = os.getenv("VTRROOT")
    posegraph = args.posegraph
    policy = args.policy
    peer_paths = glob.glob(f"{root}/torrent_ws/scripts/csv/trs_*_{posegraph}_{policy}.csv")
    seeder_paths = glob.glob(f"{root}/torrent_ws/scripts/csv/trs_*_{posegraph}_seeder.csv")
    print(f"seeder_paths {seeder_paths}")
    print(f"peer_paths {peer_paths}")

    seeders = get_seeder_dfs(seeder_paths)
    info_hashes = get_info_hashes(seeders)
    peers = get_peer_dfs(peer_paths)

    sessions = get_session_dfs(seeders, info_hashes, peers)
    pdb.set_trace()