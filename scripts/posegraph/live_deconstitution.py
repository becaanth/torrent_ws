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

import numpy as np
import pandas as pd
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message

from posegraph_utils import *

PIECE_SIZE = 2 * 1024 * 1024  # 2 MiB


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
    return pd.read_sql_query(
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

    def __init__(self, input_dir: str, output_dir: str, robot_id: str, poll_hz: float = 1.0):
        self.input_dir   = input_dir
        self.robot_id = int(robot_id)
        self.output_dir = output_dir
        self.poll_hz    = poll_hz

        os.makedirs(self.output_dir, exist_ok=True)

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

        # --- per-source row cursors (last rowid seen) ------------------------
        self._last_rowid = {k: 0 for k in self._db_relpaths}

        # --- accumulated state across polls ----------------------------------
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

        # index df read lazily on first successful connection
        self._index_df: pd.DataFrame | None = None

        print(f"[Deconstitutor] initialized with output_dir: {output_dir}")

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def _get_conn(self, key: str) -> sqlite3.Connection | None:
        """Return an open connection for key, opening it lazily if the file exists."""
        if self._conns[key] is not None:
            return self._conns[key]
        full_path = os.path.join(self.input_dir, self._db_relpaths[key])
        if not os.path.exists(full_path):
            return None
        self._conns[key] = sqlite3.connect(full_path, check_same_thread=False)
        print(f"[Deconstitutor] opened {key}")
        return self._conns[key]

    def run(self):
        print(f"[Deconstitutor] polling at {self.poll_hz} Hz  (Ctrl-C to stop)")
        try:
            while True:
                self._poll()
                time.sleep(1.0 / self.poll_hz)
        except KeyboardInterrupt:
            print("\n[Deconstitutor] stopped.")
        finally:
            self._close()

    # ------------------------------------------------------------------
    # Internal — polling
    # ------------------------------------------------------------------

    def _poll(self):
        """Read new rows from all sources, then write any new chunks."""
        if self._index_df is None:
            conn = self._get_conn('index')
            if conn is not None:
                try:
                    self._index_df = _read_full_df(conn)
                    self._last_rowid['index'] = int(self._index_df['rowid'].max()) if len(self._index_df) else 0
                except Exception as e:
                    print(f"[Deconstitutor] index: not ready yet ({e})")

        self._ingest_new_rows('vertices',      self._parse_vertices)
        self._ingest_new_rows('edges',         self._parse_edges)
        self._ingest_new_rows('pointmap',      self._parse_pointmap)
        self._ingest_new_rows('pointmap_ptr',  self._parse_pointmap_ptr)
        self._ingest_new_rows('waypoint_name', None)
        self._ingest_new_rows('env_info',      None)

        self._write_new_chunks(self.robot_id)

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
            print(f"[Deconstitutor] {key}: reconnecting ({e})")
            try:
                self._conns[key].close()
            except Exception:
                pass
            self._conns[key] = None
            return
        if new_rows.empty:
            return

        self._last_rowid[key] = int(new_rows['rowid'].max())

        # Append to accumulated DataFrame
        if self._df[key].empty:
            self._df[key] = new_rows
        else:
            self._df[key] = pd.concat([self._df[key], new_rows], ignore_index=True)

        if parse_fn is not None:
            parse_fn(new_rows)

        print(f"[Deconstitutor] {key}: +{len(new_rows)} rows "
              f"(total {len(self._df[key])})")

    # ------------------------------------------------------------------
    # Internal — id array updaters (mirrors get_db3_elements decoding)
    # ------------------------------------------------------------------

    def _parse_vertices(self, new_rows: pd.DataFrame):
        for _, row in new_rows.iterrows():
            msg = deserialize_message(row.data, get_message(row.topic_type))
            self._vertex_ids = np.append(self._vertex_ids, np.uint64(msg.id))

    def _parse_edges(self, new_rows: pd.DataFrame):
        for _, row in new_rows.iterrows():
            msg = deserialize_message(row.data, get_message(row.topic_type))
            if msg.mode.mode == 1:  # manual/teach mode only
                self._from_ids = np.append(self._from_ids, np.uint64(msg._from_id))
                self._to_ids   = np.append(self._to_ids,   np.uint64(msg._to_id))

    def _parse_pointmap(self, new_rows: pd.DataFrame):
        for _, row in new_rows.iterrows():
            msg = deserialize_message(row.data, get_message(row.topic_type))
            self._submap_ids = np.append(self._submap_ids, np.uint64(msg.vertex_id))

    def _parse_pointmap_ptr(self, new_rows: pd.DataFrame):
        for _, row in new_rows.iterrows():
            msg = deserialize_message(row.data, get_message(row.topic_type))
            self._this_vids = np.append(self._this_vids, np.uint64(msg.this_vid))
            self._map_vids  = np.append(self._map_vids,  np.uint64(msg.map_vid))

    # ------------------------------------------------------------------
    # Internal — chunk writing
    # ------------------------------------------------------------------

    def _write_new_chunks(self, robot_id):
        """
        For each submap not yet written, check if we have enough data
        to write its chunk and write it if so.
        """
        for i, sid in enumerate(self._submap_ids[:-1]):
            if i in self._written_chunks:
                continue

            sid = int(sid)

            # only write chunks originated by this robot; other pieces will have come from torrent
            originator_id = extract_robot_id(sid)
            if (originator_id != robot_id):
                self._written_chunks.add(i)
                continue

            # Rows in pointmap_ptr that belong to this submap
            ptr_row_idxs = np.where(self._map_vids == sid)[0]
            if len(ptr_row_idxs) == 0:
                continue

            # actual vertex IDs belonging to this submap
            relevant_vids = self._this_vids[ptr_row_idxs]

            # pointmap row — single row at position i in accumulated df
            if i >= len(self._df['pointmap']):
                continue
            chunk_submap = self._df['pointmap'].iloc[[i]]

            # pointmap_ptr rows
            chunk_submap_ptrs = self._df['pointmap_ptr'].iloc[ptr_row_idxs]

            # waypoint_name and env_info rows at same indices
            max_idx = int(max(ptr_row_idxs))
            if len(self._df['waypoint_name']) <= max_idx or len(self._df['env_info']) <= max_idx:
                print(f"  [chunk {i}] SKIP: waypoint_name or env_info not yet arrived")
                continue
            chunk_waypoints = self._df['waypoint_name'].iloc[ptr_row_idxs]
            chunk_env_info  = self._df['env_info'].iloc[ptr_row_idxs]

            # vertices whose vertex_id is in relevant_vids
            v_mask    = np.isin(self._vertex_ids, relevant_vids)
            valid_vtx = np.where(v_mask)[0]
            if len(valid_vtx) == 0:
                print(f"  [chunk {i}] SKIP: no matching vertices")
                continue
            sort_vidx  = np.argsort(self._vertex_ids[v_mask])
            chunk_vtxs = self._df['vertices'].iloc[valid_vtx[sort_vidx]]

            # edges whose from_id is in relevant_vids
            e_mask      = np.isin(self._from_ids, relevant_vids)
            valid_edges = np.where(e_mask)[0]
            sort_eidx   = np.argsort(self._from_ids[e_mask])
            chunk_edges = self._df['edges'].iloc[valid_edges[sort_eidx]]
            # edges can be empty for the last submap — allow it

            # --- write chunk ------------------------------------------------
            db_path = os.path.join(self.output_dir, f"{str(hex(int(sid)))[2:].zfill(16)}.db3")

            # Drop the rowid column before writing — not part of original schema
            def _drop_rowid(df: pd.DataFrame) -> pd.DataFrame:
                return df.drop(columns=['rowid'], errors='ignore')

            conn = sqlite3.connect(db_path)
            if self._index_df is not None:
                _drop_rowid(self._index_df).to_sql('vtr_index',  conn, if_exists='replace', index=False)
            _drop_rowid(chunk_vtxs).to_sql('vertices', conn, if_exists='replace', index=False)
            _drop_rowid(chunk_edges).to_sql('edges', conn, if_exists='replace', index=False)
            _drop_rowid(chunk_env_info).to_sql('env_info', conn, if_exists='replace', index=False)
            _drop_rowid(chunk_waypoints).to_sql('waypoint_name', conn, if_exists='replace', index=False)
            _drop_rowid(chunk_submap).to_sql('pointmap', conn, if_exists='replace', index=False)
            _drop_rowid(chunk_submap_ptrs).to_sql('pointmap_ptr', conn, if_exists='replace', index=False)
            conn.close()

            pad_file_to_exact_size(db_path, PIECE_SIZE)
            self._written_chunks.add(i)
            print(f"[Deconstitutor] wrote chunk {str(hex(int(sid)))[2:].zfill(16)}.db3  (submap vertex_id={sid})")

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def _close(self):
        for conn in self._conns.values():
            if conn is not None:
                conn.close()
        print("[Deconstitutor] connections closed.")


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
    print(f"ROBOT_ID : {robot_id}")

    dec = Deconstitutor(
        input_dir=input_dir,
        output_dir=output_dir,
        robot_id=robot_id,
        poll_hz=args.poll_hz,
    )
    dec.run()