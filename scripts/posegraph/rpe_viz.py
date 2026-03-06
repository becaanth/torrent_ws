import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import yaml
import pdb
import argparse

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
    def __init__(self, bag_name: str, chunks_path: str, agent: int):
        self.bag_name = bag_name
        self.chunks_path = chunks_path
        self.agent=  agent

        self.vertex_list: list[Vertex] = []
        self.edge_list: list[Edge] = []

        self._parsed_chunks: set[str] = set()

    def poll(self) -> bool:
        """
        Scan chunks_path for any .db3 not yet parsed
        Returns True if any new chunks were ingested
        """
        dirty = False
        for fname in sorted(os.listdir(self.chunks_path)):
            if not fname.endswith(".db3"):
                continue
            full_path = os.path.join(self.chunks_path, fname)
            if full_path in self._parsed_chunks:
                continue

            new_vertices, new_edges = parse_chunk(full_path)
            self.vertex_list.extend(new_vertices)
            self.edge_list.extend(new_edges)
            self._parsed_chunks.add(full_path)
            dirty = True

            print(
                f"[{self.bag_name}] parsed {fname} "
                f"(+{len(new_vertices)}v  +{len(new_edges)}e) "
                f"| total: {len(self.vertex_list)}v  {len(self.edge_list)}e"
            )

        return dirty


    def draw(self, ax: plt.Axes):
        """Render this posegraph onto ax."""
        ax.cla()
        ax.set_title(
            f"Agent {self.agent}  |  {self.bag_name}\n"
            f"{len(self.vertex_list)} vertices   {len(self.edge_list)} edges",
            fontsize=9,
        )
        ax.set_aspect("equal")
        ax.grid(True, linewidth=0.4, alpha=0.5)

        positions = self._estimate_positions()

        if not positions:
            ax.text(0.5, 0.5, "waiting for chunks…",
                    ha="center", va="center", transform=ax.transAxes, color="grey")
            return

        xs = [p[0] for p in positions.values()]
        ys = [p[1] for p in positions.values()]

        for edge in self.edge_list:
            if edge.from_id in positions and edge.to_id in positions:
                x0, y0 = positions[edge.from_id]
                x1, y1 = positions[edge.to_id]
                ax.plot([x0, x1], [y0, y1], "-", color="#4a90d9", linewidth=0.8, alpha=0.7)

        ax.scatter(xs, ys, s=12, color="#e05c5c", zorder=3, linewidths=0)

        first_id = min(positions.keys())
        ax.scatter(*positions[first_id], s=60, marker="*",
                   color="gold", zorder=4, label="start")
        ax.legend(fontsize=7, loc="upper left")


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
        roots = [e.from_id for e in self.edge_list if e.from_id not in to_ids]
        if not roots:
            return {}

        root = min(roots)
        positions: dict[int, tuple[float, float]] = {root: (0.0, 0.0)}
        current = root
        visited = {root}

        while current in adjacency:
            next_id, xi = adjacency[current]
            if next_id in visited:
                break
            dx, dy = (float(xi[0]), float(xi[1])) if xi is not None and len(xi) >= 2 else (1.0, 0.0)
            px, py = positions[current]
            positions[next_id] = (px + dx, py + dy)
            visited.add(next_id)
            current = next_id

        return positions


class PosegraphMonitor:
    """
    Polls watch_root at 1Hz for new bag subdirectories and new .db3 chunks.
    Replots all active posegraphs whenever anything changes

    Usage:
        monitor = PosegraphMonitor(watch_root, agent=0)
        monitor.run()
    """
    POLL_HZ = 1.0

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
                    chunks_path=entry.path,
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
        """Create or recreate the subplot grid to fit the current graph count."""
        n = max(len(self._graphs), 1)
        cols = min(self.plot_cols, n)
        rows = (n + cols - 1) // cols

        if self._fig is not None:
            plt.close(self._fig)

        self._fig, axes_grid = plt.subplots(
            rows, cols,
            figsize=(6 * cols, 5 * rows),
            squeeze=False,
        )
        self._fig.suptitle(
            f"Live Posegraph Monitor  |  agent {self.agent}",
            fontsize=11, fontweight="bold",
        )
        self._fig.canvas.mpl_connect(
            "key_release_event",
            lambda e: exit(0) if e.key == "escape" else None,
        )
        self._axes = axes_grid.flatten()

        # Hide any spare axes
        for ax in self._axes[n:]:
            ax.set_visible(False)

    def _redraw(self):
        """Rebuild figure if layout changed, then redraw every posegraph."""
        n = len(self._graphs)
        if self._fig is None or len(self._axes) < n:
            self._rebuild_figure()

        for idx, pg in enumerate(self._graphs.values()):
            pg.draw(self._axes[idx])

        self._fig.tight_layout()
        self._fig.canvas.draw_idle()
        plt.pause(0.001)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Live posegraph monitor")
    parser.add_argument(
        "--watch_root",
        default="/home/asrl/ASRL/vtr3/torrent_ws/deconstructed/0",
        help="Parent directory containing one subdirectory per bag run",
    )
    parser.add_argument("--agent", type=int, default=0)
    parser.add_argument("--cols", type=int, default=2)
    args = parser.parse_args()

    monitor = PosegraphMonitor(
        watch_root=args.watch_root,
        agent=args.agent,
        plot_cols=args.cols,
    )
    monitor.run()