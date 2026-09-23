"""
live_deconstruct_posegraph.py

Mirrors the behaviour of deconstruct_posegraph.py but runs continuously,
maintaining persistent read connections to the 7 source .db3 files and
only reading rows that have arrived since the last poll.

A new output chunk is written each time a new submap appears in the
pointmap table. Each chunk contains:
    - vtr_index      (read once at startup, same for all chunks)
    - vertices       (rows whose vertex_id falls in this submap's id range)
    - edges          (rows whose from_id falls in this submap's id range)
    - env_info       (rows indexed to this submap)
    - waypoint_name  (rows indexed to this submap)
    - pointmap       (the single submap row)
    - pointmap_ptr   (rows whose map_vid == submap vertex_id)

Usage:
    python live_deconstruct_posegraph.py -b <bag_name> [--agent 2] [--poll_hz 1]
"""

import argparse
import os
import sqlite3
import time
import logging

import numpy as np
import pandas as pd
import bisect
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message

from .posegraph_utils import *

PIECE_SIZE = 2 * 1024 * 1024  # 2 MiB
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_full_df(conn: sqlite3.Connection) -> pd.DataFrame:
    """Join messages + topics into a flat DataFrame."""
    return pd.read_sql_query(
        """
        SELECT t.name  AS topic_name,
               t.type  AS topic_type,
               m.rowid AS rowid,
               m.timestamp,
               m.data
        FROM   messages AS m
        JOIN   topics   AS t ON m.topic_id = t.id
        ORDER  BY m.rowid
        """,
        conn,
    )


def _read_new_rows(conn: sqlite3.Connection, after_rowid: int) -> pd.DataFrame:
    """Read only rows with rowid > after_rowid."""
    new_rows = pd.read_sql_query(
            f"""
            SELECT t.name  AS topic_name,
                t.type  AS topic_type,
                m.rowid AS rowid,
                m.timestamp,
                m.data
            FROM   messages AS m
            JOIN   topics   AS t ON m.topic_id = t.id
            WHERE  m.rowid > {after_rowid}
            ORDER  BY m.rowid
            """,
            conn,
        )
    return new_rows

# ---------------------------------------------------------------------------
# Deconstitutor
# ---------------------------------------------------------------------------

class Deconstitutor:
    """
    Maintains persistent read connections to the 7 source .db3 files of a
    VTR3 posegraph and polls for new rows at a fixed rate.

    Source layout (relative to posegraph):
        vertices/vertices_0.db3
        edges/edges_0.db3
        index/index_0.db3
        data/pointmap/pointmap_0.db3
        data/pointmap_ptr/pointmap_ptr_0.db3
        data/waypoint_name/waypoint_name_0.db3
        data/env_info/env_info_0.db3
    """

    def __init__(self, input_dir: str, output_dir: str, robot_id, poll_hz: float = 1.0):
        self.input_dir   = input_dir
        self.robot_id = int(robot_id)
        self.output_dir = output_dir
        self.poll_hz    = poll_hz

        os.makedirs(self.output_dir, exist_ok=True)

        # temp directory to write padding to files without race with seeder
        self.staging_dir = os.path.join(
            os.path.dirname(self.output_dir.rstrip('/')),
            f".staging_{os.path.basename(self.output_dir)}"
        )
        os.makedirs(self.staging_dir, exist_ok=True)

        # --- source db3 relative paths ---------------------------------------
        self._db_relpaths = {
            'vertices'     : 'vertices/vertices_0.db3',
            'edges'        : 'edges/edges_0.db3',
            'index'        : 'index/index_0.db3',
            'pointmap'     : 'data/pointmap/pointmap_0.db3',
            'pointmap_ptr' : 'data/pointmap_ptr/pointmap_ptr_0.db3',
            'waypoint_name': 'data/waypoint_name/waypoint_name_0.db3',
            'env_info'     : 'data/env_info/env_info_0.db3',
        }

        # Connections opened lazily once each file's directory is created by VTR3
        self._conns: dict[str, sqlite3.Connection | None] = {k: None for k in self._db_relpaths}

        # last rowid seen
        self._last_rowid = {k: 0 for k in self._db_relpaths}

        # accumulated state across polls
        self._df: dict[str, pd.DataFrame] = {k: pd.DataFrame() for k in self._db_relpaths}

        # Decoded id arrays (mirrors deconstruct_posegraph.py)
        self._vertex_ids   : np.ndarray = np.array((), dtype=np.uint64)
        self._from_ids     : np.ndarray = np.array((), dtype=np.uint64)
        self._to_ids       : np.ndarray = np.array((), dtype=np.uint64)
        self._submap_ids   : np.ndarray = np.array((), dtype=np.uint64)
        self._this_vids    : np.ndarray = np.array((), dtype=np.uint64)
        self._map_vids     : np.ndarray = np.array((), dtype=np.uint64)

        # Which submap indices have already been written as output chunks
        self._written_chunks: set[int] = set()

        # track local submaps
        self._pending_positions: list[int] = []   # anchor looked foreign; unresolved until pointmap_ptr arrives
        self._local_submaps_positions: list[int] = []
        self._transition_grace_active = False
        self._grace_armed_by: int | None = None

        # index df read lazily on first successful connection
        self._index_df: pd.DataFrame | None = None

        logging.info(f"initialized with IP {self.input_dir} OP: {self.output_dir}")

    def _get_conn(self, key: str) -> sqlite3.Connection | None:
        """Return an open connection for key, opening it lazily if the file exists."""
        if self._conns[key] is not None:
            return self._conns[key]
        full_path = os.path.join(self.input_dir, self._db_relpaths[key])
        if not os.path.exists(full_path):
            return None
        self._conns[key] = sqlite3.connect(full_path, check_same_thread=False)
        logging.info(f"opened {key}")
        return self._conns[key]

    def run(self):
        logging.info(f"polling at {self.poll_hz} Hz  (Ctrl-C to stop)")
        try:
            while True:
                self._poll()
                time.sleep(1.0 / self.poll_hz)
        except KeyboardInterrupt:
            logging.info("\nstopped.")
        finally:
            self._close()

    def _poll(self):
        """Read new rows from all sources, then write any new chunks."""
        conn = self._get_conn('index')
        if conn is not None:
            try:
                self._index_df = _read_full_df(conn)
                self._last_rowid['index'] = int(self._index_df['rowid'].max()) if len(self._index_df) else 0
            except Exception as e:
                logging.warning(f"index: not ready yet ({e})")
        else:
            logging.warning(f"conn is None")

        self._ingest_new_rows('vertices',      self._parse_vertices)
        self._ingest_new_rows('edges',         self._parse_edges)
        self._ingest_new_rows('pointmap',      self._parse_pointmap)
        self._ingest_new_rows('pointmap_ptr',  self._parse_pointmap_ptr)
        self._ingest_new_rows('waypoint_name', None)
        self._ingest_new_rows('env_info',      None)

        if self._index_df is not None and not self._index_df.empty:
            self._write_new_chunks()
        # else:
        #     logging.warning("Delaying chunk writing, waiting for valid index structure.")

    def _ingest_new_rows(self, key: str, parse_fn):
        """
        Read rows added since last poll, append to self._df[key],
        and call parse_fn to update decoded id arrays.
        Silently skips if the file does not exist yet or the schema isn't ready.
        """
        conn = self._get_conn(key)
        if conn is None:
            return
        try:
            new_rows = _read_new_rows(conn, self._last_rowid[key])
        except Exception as e:
            logging.info(f"_ingest_new_rows exception {e}")
            return
        if new_rows.empty:
            return

        self._last_rowid[key] = int(new_rows['rowid'].max())

        # Run parsing first to filter rows if parse_fn returns a modified DF
        if parse_fn is not None:
            parsed_rows = parse_fn(new_rows)
            if parsed_rows is not None:
                new_rows = parsed_rows

        # Append filtered rows to accumulated DataFrame
        if self._df[key].empty:
            self._df[key] = new_rows
        else:
            self._df[key] = pd.concat([self._df[key], new_rows], ignore_index=True)

    # parse functions for each datatype
    def _parse_vertices(self, new_rows: pd.DataFrame):
        for _, row in new_rows.iterrows():
            msg = deserialize_message(row.data, get_message(row.topic_type))
            self._vertex_ids = np.append(self._vertex_ids, np.uint64(msg.id))

    def _parse_edges(self, new_rows: pd.DataFrame):
        for _, row in new_rows.iterrows():
            msg = deserialize_message(row.data, get_message(row.topic_type))
            self._from_ids = np.append(self._from_ids, np.uint64(msg._from_id))
            self._to_ids   = np.append(self._to_ids,   np.uint64(msg._to_id))
    
    def _parse_pointmap(self, new_rows: pd.DataFrame):
        for _, row in new_rows.iterrows():
            msg = deserialize_message(row.data, get_message(row.topic_type))
            sid = np.uint64(msg.vertex_id)
            self._submap_ids = np.append(self._submap_ids, sid)
            idx = len(self._submap_ids) - 1
            if extract_robot_id(int(sid)) == self.robot_id:
                # maintain a list of indices to self._submap_ids for local maps
                self._local_submaps_positions.append(idx)
            else:
                self._pending_positions.append(idx)

    def _parse_pointmap_ptr(self, new_rows: pd.DataFrame):
        for _, row in new_rows.iterrows():
            msg = deserialize_message(row.data, get_message(row.topic_type))
            self._this_vids = np.append(self._this_vids, np.uint64(msg.this_vid))
            self._map_vids  = np.append(self._map_vids,  np.uint64(msg.map_vid))

    def _resolve_pending(self):
        """Recheck anchor-foreign submaps once their member vertices are known.
        Runs before the main write loop; typically a no-op (empty list) except
        right after a branch event."""
        still_pending = []
        for i in self._pending_positions:
            sid = int(self._submap_ids[i])
            ptr_row_idxs = np.where(self._map_vids == sid)[0]
            if len(ptr_row_idxs) == 0:
                still_pending.append(i)   # pointmap_ptr not arrived yet, retry next poll
                continue
            member_vids = self._this_vids[ptr_row_idxs]
            if any(extract_robot_id(int(v)) == self.robot_id for v in member_vids):
                bisect.insort(self._local_submaps_positions, i)  # branch anchor -- keep index order
                if not self._transition_grace_active:
                    self._transition_grace_active = True
                    self._grace_armed_by = i
            # else: genuinely someone else's submap, drop permanently -- nothing to do
        self._pending_positions = still_pending

    def _write_new_chunks(self):
        """
        For each submap not yet written, check if we have enough data
        to write its chunk and write it if so.
        """
        self._resolve_pending()
        if not self._local_submaps_positions:
            return

        # dont touch the in progress piece
        last_local_idx = self._local_submaps_positions[-1]

        for i in self._local_submaps_positions:
            if i in self._written_chunks:
                continue

            sid = int(self._submap_ids[i])

            # Rows in pointmap_ptr that belong to this submap
            ptr_row_idxs = np.where(self._map_vids == sid)[0]
            if len(ptr_row_idxs) == 0:
                continue

            # actual vertex IDs belonging to this submap
            relevant_vids = self._this_vids[ptr_row_idxs]
            logger.info(f"relevant_vids {relevant_vids}")

            # pointmap row — single row at position i in accumulated df
            if i >= len(self._df['pointmap']):
                continue
            chunk_submap = self._df['pointmap'].iloc[[i]]

            # pointmap_ptr rows
            chunk_submap_ptrs = self._df['pointmap_ptr'].iloc[ptr_row_idxs]

            # waypoint_name and env_info rows at same indices
            max_idx = int(max(ptr_row_idxs))
            if len(self._df['waypoint_name']) <= max_idx or len(self._df['env_info']) <= max_idx:
                logging.debug(f"  [chunk {i}] SKIP: waypoint_name or env_info not yet arrived")
                continue
            chunk_waypoints = self._df['waypoint_name'].iloc[ptr_row_idxs]
            chunk_env_info  = self._df['env_info'].iloc[ptr_row_idxs]

            # vertices whose vertex_id is in relevant_vids
            v_mask    = np.isin(self._vertex_ids, relevant_vids)
            valid_vtx = np.where(v_mask)[0]
            if len(valid_vtx) == 0:
                logging.debug(f"  [chunk {i}] SKIP: no matching vertices")
                continue
            sort_vidx  = np.argsort(self._vertex_ids[v_mask])
            chunk_vtxs = self._df['vertices'].iloc[valid_vtx[sort_vidx]]

            # edges whose from_id is in relevant_vids
            # edges can be empty for the last submap — allow it
            from_mask = np.isin(self._from_ids, relevant_vids)
            to_mask = np.isin(self._to_ids, relevant_vids)
            e_mask = from_mask | to_mask
            valid_edges = np.where(e_mask)[0]
            sort_eidx   = np.argsort(self._from_ids[e_mask])
            chunk_edges = self._df['edges'].iloc[valid_edges[sort_eidx]]

            if i == last_local_idx:
                # If this is the last submap, evaluate if it constitutes a merge
                logging.info("this is the last submap")
                
                merges_to_remote = False
                from_ids_i = self._from_ids[e_mask]
                to_ids_i = self._to_ids[e_mask]

                for f_id, t_id in zip(from_ids_i, to_ids_i):
                    f_id, t_id = int(f_id), int(t_id)
                    if extract_robot_id(f_id) != extract_robot_id(t_id):
                        merges_to_remote = True
                        logging.info(f"merging local to remote! edge {f_id} -> {t_id}")
                        break
                    elif extract_major_id(f_id) != extract_major_id(t_id):
                        merges_to_remote = True
                        logging.info(f"merging local to local! edge {f_id} -> {t_id}")
                        break
                if not merges_to_remote:
                    # nothing to merge, ignore the current submap
                    logging.info("skipping, no merges to remote")
                    continue
            else:
                merges_to_remote = False # not a boundary map

            # non-manual edge filter, except for merge/branch boundaries
            if merges_to_remote and not self._transition_grace_active:
                self._transition_grace_active = True
                self._grace_armed_by = i


            # non-manual gate:
            if not (merges_to_remote or self._transition_grace_active) and len(chunk_edges) > 0:
                edge_modes = [inspect_ros_data(e).mode.mode for _, e in chunk_edges.iterrows()]
                if any(mode != 1 for mode in edge_modes):
                    logger.info(f"skipping non-manual piece")
                    self._written_chunks.add(i)
                    continue
            elif (self._transition_grace_active and len(chunk_edges) > 0 and i != self._grace_armed_by):
                edge_modes = [inspect_ros_data(e).mode.mode for _, e in chunk_edges.iterrows()]
                if all(mode == 1 for mode in edge_modes):
                    self._transition_grace_active = False   # manual driving has genuinely resumed
                    self._grace_armed_by = None
                    
            # --- write chunk ------------------------------------------------
            filename = f"{str(hex(int(sid)))[2:].zfill(16)}.db3"
            staging_path = os.path.join(self.staging_dir, filename)
            final_path = os.path.join(self.output_dir, filename)

            # Drop the rowid column before writing — not part of original schema
            def _drop_rowid(df: pd.DataFrame) -> pd.DataFrame:
                return df.drop(columns=['rowid'], errors='ignore')

            conn = sqlite3.connect(staging_path)
            if self._index_df is not None:
                _drop_rowid(self._index_df).to_sql('vtr_index',  conn, if_exists='replace', index=False)
            _drop_rowid(chunk_vtxs).to_sql('vertices', conn, if_exists='replace', index=False)
            _drop_rowid(chunk_edges).to_sql('edges', conn, if_exists='replace', index=False)
            _drop_rowid(chunk_env_info).to_sql('env_info', conn, if_exists='replace', index=False)
            _drop_rowid(chunk_waypoints).to_sql('waypoint_name', conn, if_exists='replace', index=False)
            _drop_rowid(chunk_submap).to_sql('pointmap', conn, if_exists='replace', index=False)
            _drop_rowid(chunk_submap_ptrs).to_sql('pointmap_ptr', conn, if_exists='replace', index=False)
            conn.close()

            # for _, e in chunk_edges.iterrows():
            #     logging.info(f"chunk_edges {inspect_ros_data(e)}")

            pad_file_to_exact_size(staging_path, PIECE_SIZE)
            os.rename(staging_path, final_path)
            self._written_chunks.add(i)
            logging.debug(f"finalized chunk {filename}")

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def _close(self):
        for conn in self._conns.values():
            if conn is not None:
                conn.close()
        logging.info("connections closed.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Live posegraph deconstructor")
    parser.add_argument('-p', '--posegraph', required=True,help="posegraph name (subdirectory under folder_path)")
    parser.add_argument('--posegraph_root', default=f'{os.getenv("VTRTEMP")}/pgs')
    parser.add_argument('--piece_root', default=f'{os.getenv("VTRTEMP")}/pcs')
    parser.add_argument('--poll_hz', type=float, default=1.0)
    args = parser.parse_args()

    robot_id = os.getenv("ROBOT_ID")
    input_dir   = os.path.join(args.posegraph_root, args.posegraph, 'graph')
    output_dir = os.path.join(args.piece_root, args.posegraph, robot_id)
    logging.info(f"ROBOT_ID : {robot_id}")

    dec = Deconstitutor(
        input_dir=input_dir,
        output_dir=output_dir,
        robot_id=robot_id,
        poll_hz=args.poll_hz,
    )
    dec.run()