import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import yaml
import pdb
import argparse
import pylgmath

import threading
import time
from dataclasses import dataclass, field
from typing import Optional

from rclpy.serialization import deserialize_message, serialize_message
from rosidl_runtime_py.utilities import get_message

# # folder_path = '/home/asrl/ASRL/vtr3/torrent_ws'
# folder_path = '/home/asrl/ASRL/vtr3'
# chunk_name =  'torrent_ws/deconstructed/0/' + bag_name
# chunks_path = f'{folder_path}/{chunk_name}'


def inspect_ros_data(frame):
    msg = deserialize_message(frame.data, get_message(frame["topic_type"]))
    return msg

@dataclass
class Vertex:
    vertex_id: int

@dataclass
class Edge:
    from_id: int
    to_id: int
    xi: Optional[np.ndarray] = None
    cov: Optional[np.ndarray] = None

    def __repr__(self):
        return f"Edge({self.from_id}) -> ({self.to_id})"
    

# Helper functions
def _deserialize_frame(row: pd.Series):
    """Deserialize a single messages-table row into a ROS message."""
    return deserialize_message(row["data"], get_message(row["topic_type"]))


def _read_full_df(db_path: str) -> pd.DataFrame:
    """Join messages + topics from a .db3 file into a flat DataFrame."""
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(
        """
        SELECT t.name AS topic_name,
               t.type AS topic_type,
               m.timestamp,
               m.data
        FROM   messages AS m
        JOIN   topics   AS t ON m.topic_id = t.id
        ORDER  BY m.timestamp
        """,
        conn,
    )
    conn.close()
    return df


def parse_chunk(db_path: str) -> tuple[list[Vertex], list[Edge]]:
    """
    Parse a single deconstructed .db3 chunk.
    Returns (vertices, edges) extracted from that chunk.
    """
    conn = sqlite3.connect(db_path)

    vertices: list[Vertex] = []
    edges: list[Edge] = []

    # --- vertices -----------------------------------------------------------
    try:
        vtx_df = pd.read_sql_query(
            "SELECT topic_name, topic_type, timestamp, data FROM vertices", conn
        )
        for _, row in vtx_df.iterrows():
            try:
                msg = deserialize_message(row["data"], get_message(row["topic_type"]))
                vertices.append(Vertex(vertex_id=int(msg.id)))
            except Exception as exc:
                print(f"[parse_chunk] vertex deserialize error in {db_path}: {exc}")
    except Exception as exc:
        print(f"[parse_chunk] no vertices table in {db_path}: {exc}")

    # --- edges --------------------------------------------------------------
    try:
        edge_df = pd.read_sql_query(
            "SELECT topic_name, topic_type, timestamp, data FROM edges", conn
        )
        for _, row in edge_df.iterrows():
            try:
                msg = deserialize_message(row["data"], get_message(row["topic_type"]))
                # Only teach-mode edges (mode == 1)
                if msg.mode.mode != 1:
                    continue
                xi = np.array(msg.t_to_from.xi)
                cov = np.array(msg.t_to_from.cov).reshape(6, 6)
                edges.append(Edge(from_id=int(msg.from_id), to_id=int(msg.to_id), xi=xi, cov=cov))
            except Exception as exc:
                print(f"[parse_chunk] edge deserialize error in {db_path}: {exc}")
    except Exception as exc:
        print(f"[parse_chunk] no edges table in {db_path}: {exc}")

    conn.close()
    return vertices, edges


class Posegraph:
    """
    Represents one agents deconstructed directory
    """
    def __init__(self, bag_name: str, data_path: str, topology_path: str, agent: int):
        self.bag_name = bag_name
        self.data_path = data_path
        self.topology_path = topology_path
        self.agent = agent

        self.vertex_list: list[Vertex] = []
        self.edge_list: list[Edge] = []
        self.data_edge_list: list[Edge] = []

        self._parsed_data: set[str] = set()
        self._parsed_topology: set[str] = set()

    def poll(self) -> bool:
        dirty = False

        # --- Pass 1: topology (unchanged) ---
        for fname in sorted(os.listdir(self.topology_path)):
            if not fname.endswith(".db3"):
                continue
            full_path = os.path.join(self.topology_path, fname)
            if full_path in self._parsed_topology:
                continue
            if os.path.exists(full_path + "-journal"):
                continue

            new_vertices, new_edges = parse_chunk(full_path)
            if not new_vertices and not new_edges:
                continue

            self.vertex_list.extend(new_vertices)
            self.edge_list.extend(new_edges)
            self._parsed_topology.add(full_path)
            dirty = True

            print(
                f"[{self.bag_name}] parsed topology {fname} "
                f"(+{len(new_vertices)}v  +{len(new_edges)}e)"
            )

        # --- Pass 2: data (retry every poll until found) ---
        for fname in sorted(os.listdir(self.topology_path)):
            if not fname.endswith(".db3"):
                continue
            topology_full_path = os.path.join(self.topology_path, fname)
            if topology_full_path not in self._parsed_topology:
                continue  # topology not ready yet, skip

            data_full_path = os.path.join(self.data_path, fname)
            if data_full_path in self._parsed_data:
                continue  # already got this one

            if not os.path.exists(data_full_path):
                continue  # not written yet, will retry next poll

            if os.path.exists(data_full_path + "-journal"):
                continue  # still writing

            _, new_edges = parse_chunk(data_full_path)
            if not new_edges:
                continue  # not ready, retry

            self.data_edge_list.extend(new_edges)
            self._parsed_data.add(data_full_path)
            dirty = True

            print(f"[{self.bag_name}] parsed data {fname} (+{len(new_edges)}e)")

        return dirty

    def draw(self, ax: plt.Axes):
        positions = self._estimate_positions()
        if not positions:
            return

        # Convert to numpy arrays for masking
        keys = sorted(positions.keys())
        data = np.array([positions[k] for k in keys]) # shape (N, 3)
        xs, ys, received = data[:, 0], data[:, 1], data[:, 2]

        # 1. Plot the "Base" line (The whole path in grey)
        # ax.plot(xs, ys, "-", color="grey", linewidth=0.8, alpha=0.3)

        # 2. Plot the "Received" line (Masking out the zeros)
        # Mask is True where we want to HIDE data (where received == 0)
        mask = (received == 0)
        m_xs = np.ma.masked_where(mask, xs)
        m_ys = np.ma.masked_where(mask, ys)

        line, = ax.plot(m_xs, m_ys, "-", linewidth=1.2, label=self.bag_name)
        color = line.get_color()

        # 3. Scatter points (Colored if received, grey if not)
        colors = [color if r else "grey" for r in received]
        ax.scatter(xs, ys, s=8, color=colors, zorder=3, linewidths=0)

        # 4. Start marker
        ax.scatter(xs[0], ys[0], s=60, marker="*", color=color, zorder=4)

    def _estimate_positions(self) -> dict[int, tuple[float, float]]:
        """
        Placeholder: approximate 2D positions by integrating xi[0:2] along
        the edge chain. Replace with proper pylgmath SE3 chaining when ready.
        """
        if not self.edge_list:
            return {}

        adjacency: dict[int, tuple[int, Optional[np.ndarray]]] = {
            e.from_id: (e.to_id, e.xi) for e in self.edge_list
        }

        to_ids = {e.to_id for e in self.edge_list}
        data_to_ids = {e.to_id for e in self.data_edge_list}

        roots = [e.from_id for e in self.edge_list if e.from_id not in to_ids]
        if not roots:
            return {}

        root = min(roots)
        positions: dict[int, tuple[float, float]] = {root: (0.0, 0.0, 0)}
        current = root
        visited = {root}

        T_curr = pylgmath.Transformation()
        while current in adjacency:
            next_id, xi = adjacency[current]
            if next_id in visited:
                break

            data_received = 0
            if next_id in data_to_ids:
                data_received = 1
                
            # dx, dy = (float(xi[0]), float(xi[1])) if xi is not None and len(xi) >= 2 else (1.0, 0.0)
            T_edge = pylgmath.Transformation( # TODO: covariance?
               xi_ab=xi.reshape(6,1)
            )
            T_curr = T_curr @ T_edge  # compose in world frame
            r = T_curr.r_ab_inb().flatten()
            positions[next_id] = (r[0].item(), r[1].item(), data_received)
            visited.add(next_id)
            current = next_id

        return positions
    

    def save(self, path=None):
        """
        Pickle this Posegraph to disk.
        Defaults to <chunks_path>/<bag_name>.pkl if no path given.
        Returns the path written to.
        """
        import pickle
        if path is None:
            path = os.path.join(self.chunks_path, f"{self.bag_name}.pkl")
        with open(path, "wb") as f:
            pickle.dump(self, f)
        print(f"[{self.bag_name}] saved to {path}")
        return path

    @staticmethod
    def load(path):
        """
        Load a pickled Posegraph from disk.

        Example usage in a notebook / debug session:
            pg = Posegraph.load('my_bag.pkl')
            print(pg.edge_list[:5])
        """
        import pickle
        with open(path, "rb") as f:
            pg = pickle.load(f)
        print(f"[{pg.bag_name}] loaded from {path} "
              f"({len(pg.vertex_list)}v  {len(pg.edge_list)}e)")
        return pg


class PosegraphMonitor:
    """
    Polls watch_root at 1Hz for new bag subdirectories and new .db3 chunks.
    Replots all active posegraphs whenever anything changes

    Usage:
        monitor = PosegraphMonitor(watch_root, agent=0)
        monitor.run()
    """
    POLL_HZ = 4.0

    def __init__(self, watch_root: str, agent: int = 0, plot_cols: int = 2):
        self.watch_root = watch_root
        self.agent = agent
        self.plot_cols = plot_cols

        self._graphs: dict[str, Posegraph] = {}
        self._fig: Optional[plt.Figure] = None
        self._axes: Optional[np.ndarray] = None  # flattened axes array

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def run(self):
        os.makedirs(self.watch_root, exist_ok=True)
        plt.ion()
        print(f"[PosegraphMonitor] watching {self.watch_root}  ({self.POLL_HZ} Hz)")

        try:
            while True:
                dirty = self._poll()
                if dirty:
                    self._redraw()
                plt.pause(1.0 / self.POLL_HZ)
        except KeyboardInterrupt:
            print("\n[PosegraphMonitor] stopped.")
        finally:
            plt.ioff()
            plt.show()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _poll(self) -> bool:
        """
        1. Check for new bag subdirectories under watch_root.
        2. For each known graph, poll for new chunks.
        Returns True if anything changed.
        """
        dirty = False

        # Discover new bag directories
        for entry in os.scandir(self.watch_root):
            if entry.is_dir() and entry.name not in self._graphs:
                pg = Posegraph(
                    bag_name=entry.name,
                    # chunks_path=f"{entry.path}",
                    data_path=f"{entry.path}/data",
                    topology_path=f"{entry.path}/topology",
                    agent=self.agent,
                )
                self._graphs[entry.name] = pg
                print(f"[PosegraphMonitor] new bag: {entry.name}")
                dirty = True

        # Poll each known graph for new chunks
        for pg in self._graphs.values():
            if pg.poll():
                dirty = True

        return dirty


    def _rebuild_figure(self):
        """Create a single shared axes for all posegraphs."""
        if self._fig is not None:
            plt.close(self._fig)
        self._fig, ax = plt.subplots(figsize=(12, 8))
        self._fig.suptitle(
            f"Live Posegraph Monitor  |  agent {self.agent}",
            fontsize=11, fontweight="bold",
        )
        self._fig.canvas.mpl_connect(
            "key_release_event",
            lambda e: exit(0) if e.key == "escape" else None,
        )
        self._ax = ax
        self._ax.set_aspect("equal")
        self._ax.grid(True, linewidth=0.4, alpha=0.5)

    def _redraw(self):
        """Clear the shared axes and redraw all posegraphs."""
        if self._fig is None:
            self._rebuild_figure()

        self._ax.cla()
        # self._ax.set_aspect("equal")
        self._ax.set_xlim([-200,80])
        self._ax.set_ylim([-180,100])
        self._ax.grid(True, linewidth=0.4, alpha=0.5)
        self._ax.set_title(
            f"{len(self._graphs)} posegraph(s)  |  agent {self.agent}",
            fontsize=9,
        )

        if not self._graphs:
            self._ax.text(0.5, 0.5, "waiting for bags…",
                         ha="center", va="center", transform=self._ax.transAxes, color="grey")
        else:
            for pg in self._graphs.values():
                pg.draw(self._ax)
            self._ax.legend(fontsize=8, loc="upper left",markerscale=5)

        self._fig.tight_layout()
        self._fig.canvas.draw_idle()
        plt.pause(0.001)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Live posegraph monitor")
    # parser.add_argument(
    #     "--watch_root",
    #     default="/home/asrl/ASRL/vtr3/torrent_ws/deconstructed/0",
    #     help="Parent directory containing one subdirectory per bag run",
    # )
    parser.add_argument("--agent", type=int, default=0)
    args = parser.parse_args()
    watch_root = f"/home/asrl/ASRL/vtr3/torrent_ws/deconstructed/{args.agent}"

    monitor = PosegraphMonitor(
        watch_root=watch_root,
        agent=args.agent,
    )
    monitor.run()