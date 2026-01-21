# torrent_ws

`torrent_ws` implements submap-level pose graph sharing for a multi-robot fleet
using a BitTorrent-style distribution mechanism integrated with ROS 2.

Each robot runs the same ROS node and cooperatively exchanges pose-graph submaps
("pieces") using gossip, scheduling, and libtorrent.

---

## Scripts

### scripts/deconstruct_posegraph.py

Reads a pose graph from `$VTRTEMP`, splits it into submap-level pieces, and stores
them at:

```
deconstructed/<robot_id>/<posegraph_name>/
```

Example:

```
deconstructed/0/woody_convoy/
```

`robot_id` identifies the robot that originally generated the pose graph.

---

### scripts/reconstruct_posegraph.py

Reads submap pieces from:

```
deconstructed/<robot_id>/<posegraph_name>/
```

Reassembles them into a valid pose graph and writes the result to:

```
reconstructed/<robot_id>/<posegraph_name>/
```

---

## ROS Package: torrent_pkg

### registry_node.py

`RegistryNode` is the main ROS 2 node run on every robot in the fleet.

Each instance performs the following:

1. Publishes local piece possession via gossip
2. Receives gossip from other robots and maintains a local registry
3. Determines which piece to request next and from which peer
4. Requests or serves magnet links using ROS services
5. Initiates BitTorrent downloads via libtorrent

All robots run the same node; behavior is determined by `robot_id`.

---

## Internal Architecture

`RegistryNode` is composed of several modular components.

### PieceState

- Authority on local piece possession
- Only component allowed to read from the filesystem
- Scans available submap files at startup
- Provides thread-safe access to:
  - owned pieces
  - missing pieces
  - bitfield representation for gossip

Used by the Scheduler and gossip publisher.

---

### GossipInterface

- Maintains a local view of which peers have which pieces
- Updated from incoming gossip messages
- Provides:
  - known peers
  - per-piece availability across the fleet

---

### Scheduler

- Compares local state (PieceState) with fleet knowledge (GossipInterface)
- Selects the next piece to download
- Tracks in-flight requests to avoid duplicates

Current policy (21-01):

Request the lowest-index missing piece that at least one peer advertises.

---

### MagnetService

- ROS service interface for magnet link exchange
- Responsibilities:
  - Request a magnet link from a peer for (peer_id, piece_id)
  - Serve magnet links to requesting peers

Acts as a bridge between the Scheduler and TorrentManager.

---

### TorrentManager

- Wraps libtorrent
- Runs in its own thread
- Handles:
  - seeding local pieces
  - downloading pieces via magnet links
  - tracking torrent progress and completion

---

## System Overview Diagram

![Registry Diagram](docs/registry_diagram.png)