from posegraph.posegraph_utils import *
import pdb
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Script to deconstruct posegraphs submap-wise')
    parser.add_argument('-p', '--posegraph', default='none', help="The name of the posegraph") 
    parser.add_argument('--piece_root', default='/home/asrl/ASRL/vtr3/temp/pcs')
    args = parser.parse_args()

    piece_path = os.path.join(args.piece_root, args.posegraph)

    pdb.set_trace()