import argparse
# import matplotlib.pyplot as plt
import numpy as np

from posegraph_utils import *

ORIGINAL_ROOT      = '/home/asrl/ASRL/vtr3/temp/pgs'
RECONSTRUCTED_ROOT = '/home/asrl/ASRL/vtr3/temp/pgs'


def compare_posegraphs(bag_name: str):
    topics = ['vertices', 'edges', 'index', 'pointmap', 'pointmap_ptr', 'waypoint_name', 'env_info']

    orig  = {t: get_db3_elements(f'{ORIGINAL_ROOT}/{bag_name}/graph',       t) for t in topics}
    recon = {t: get_db3_elements(f'{RECONSTRUCTED_ROOT}/r{bag_name}/graph', t) for t in topics}

    # ------------------------------------------------------------------ vertices
    orig_vids  = orig['vertices']['vertex_ids']
    recon_vids = recon['vertices']['vertex_ids']

    orig_vid_to_idx  = {int(v): i for i, v in enumerate(orig_vids)}
    recon_vid_to_idx = {int(v): i for i, v in enumerate(recon_vids)}

    all_vids    = sorted(set(orig_vid_to_idx) | set(recon_vid_to_idx))
    vertex_match = []
    for vid in all_vids:
        in_o = vid in orig_vid_to_idx
        in_r = vid in recon_vid_to_idx
        if in_o and in_r:
            data_o = orig['vertices']['df'].iloc[orig_vid_to_idx[vid]]['data']
            data_r = recon['vertices']['df'].iloc[recon_vid_to_idx[vid]]['data']
            match  = data_o == data_r
        else:
            match = False
        vertex_match.append(match)
        if not match:
            print(f'  vertex {vid:#018x}: orig={in_o} recon={in_r} match={match}')

    # -------------------------------------------------------------------- edges
    orig_from  = orig['edges']['from_ids']
    orig_to    = orig['edges']['to_ids']
    recon_from = recon['edges']['from_ids']
    recon_to   = recon['edges']['to_ids']

    orig_edge_df  = orig['edges']['df'].reset_index(drop=True)
    recon_edge_df = recon['edges']['df'].reset_index(drop=True)

    orig_edge_map  = {(int(f), int(t)): i for i, (f, t) in enumerate(zip(orig_from,  orig_to))}
    recon_edge_map = {(int(f), int(t)): i for i, (f, t) in enumerate(zip(recon_from, recon_to))}

    all_edges  = sorted(set(orig_edge_map) | set(recon_edge_map))
    edge_match = []
    for eid in all_edges:
        in_o = eid in orig_edge_map
        in_r = eid in recon_edge_map
        if in_o and in_r:
            data_o = orig_edge_df.iloc[orig_edge_map[eid]]['data']
            data_r = recon_edge_df.iloc[recon_edge_map[eid]]['data']
            match  = data_o == data_r
        else:
            match = False
        edge_match.append(match)
        if not match:
            print(f'  edge ({eid[0]:#018x} -> {eid[1]:#018x}): orig={in_o} recon={in_r} match={match}')

    # --------------------------------------------------------------- blob tables
    def blobs_equal(key):
        return orig[key]['df']['data'].reset_index(drop=True).equals(
               recon[key]['df']['data'].reset_index(drop=True))

    blob_results = {k: blobs_equal(k) for k in ['index', 'env_info', 'waypoint_name', 'pointmap', 'pointmap_ptr']}

    # ----------------------------------------------------------------------- summary
    print(f'\nvertices:      {all(vertex_match)}  ({sum(vertex_match)}/{len(vertex_match)} match)')
    print(f'edges:         {all(edge_match)}  ({sum(edge_match)}/{len(edge_match)} match)')
    for k, v in blob_results.items():
        print(f'{k:<18} {v}')

    # ----------------------------------------------------------------------- plot
    # plt.figure()
    # plt.plot(orig_vids,  label='original',      marker='.', linestyle='none')
    # plt.plot(recon_vids, label='reconstructed', marker='.', linestyle='none')
    # plt.xlabel('index in .db3')
    # plt.ylabel('vertex id')
    # plt.legend()
    # plt.tight_layout()
    # plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Compare original and reconstructed posegraphs')
    parser.add_argument('-b', '--bag_name', required=True, help='Posegraph bag name')
    args = parser.parse_args()
    compare_posegraphs(args.bag_name)
