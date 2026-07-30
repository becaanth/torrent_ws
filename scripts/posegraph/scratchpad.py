from posegraph.posegraph_utils import *
import pdb
import argparse

# python3 posegraph/scratchpad.py -p dome_loop2 -s 3 -t 15 -f fb54489e3f8600d7.db3 -d indro

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Script to deconstruct posegraphs submap-wise')
    parser.add_argument('-p', '--posegraph', default='none', help="The name of the posegraph") 
    parser.add_argument('-t', '--this_robot', default='none', help="The name of the posegraph") 
    parser.add_argument('-s', '--src_robot', default='none', help="The name of the posegraph") 
    parser.add_argument('-f', '--db', default='none', help="The name of the posegraph") 
    parser.add_argument('-d', '--device', default='none', help="The name of the posegraph") 
    args = parser.parse_args()

    piece_path = f"/home/{args.device}/ASRL/vtr3/temp/pcs"
    db_path = f"{piece_path}/{args.posegraph}_{args.this_robot}/{args.src_robot}/{args.db}"
    print(f"db_path: {db_path}")
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