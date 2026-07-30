from posegraph.posegraph_utils import *
import pdb
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Script to deconstruct posegraphs submap-wise')
    parser.add_argument('-p', '--posegraph', default='none', help="The name of the posegraph") 
    parser.add_argument('-a', '--this_robot', default='none', help="The name of the posegraph") 
    parser.add_argument('-b', '--src_robot', default='none', help="The name of the posegraph") 
    parser.add_argument('-d', '--db', default='none', help="The name of the posegraph") 
    parser.add_argument('--piece_root', default='/home/asrl/ASRL/vtr3/temp/pcs')
    args = parser.parse_args()

    piece_path = os.path.join(args.piece_root, args.posegraph)
    db_path = f"{piece_path}_{args.this_robot}/{args.src_robot}/{args.db}"
    tables = ['vtr_index','env_info','waypoint_name','vertices','edges','pointmap','pointmap_ptr']
    conn = sqlite3.connect(db_path, isolation_level=None)
    poll_data = {}

    for table in tables:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
            poll_data[table] = df
        except Exception as e:
            # logging.debug(f"_parse_piece id | {db_file} | {e}")
            poll_data[table] = pd.DataFrame() 

    conn.close()

    print(poll_data)
    pdb.set_trace()