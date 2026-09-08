from enum import Enum
import numpy as np

import sys
from pathlib import Path

# Add the 'scripts' directory to sys.path
SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.append(str(SCRIPTS_DIR))

# Import the module
from torrent.piece_pickers import *

PIECE_SIZE = 2 * 1024 * 1024 # same as real system

class Status(Enum):
    UNAWARE = 0
    TOPOLOGY=1
    FULL_DATA=2

class Role(Enum):
    TEACH = 0
    REPEAT = 1

class Submap:
    """
    an atomic submap, with a position and state=1 for topology, state=2 for full data
    """
    def __init__(self, rid, x, y, idx, state, size_bytes=PIECE_SIZE):
        self.rid = rid
        # metric position
        self.x = x
        self.y = y
        # topological position
        self.idx = idx # index in this session
        self.state = state
        self.size_bytes = size_bytes
        self.bytes_received = size_bytes if state == Status.FULL_DATA else 0

    @property
    def is_complete(self):
        return self.state == Status.FULL_DATA

    def add_bytes(self, n):
        if self.state == Status.FULL_DATA or n <= 0:
            return
        self.bytes_received = min(self.size_bytes, self.bytes_received + n)
        if self.bytes_received >= self.size_bytes:
            self.state = Status.FULL_DATA

    def __repr__(self):
        return f"robot: {self.rid}, idx: {self.idx}, state: {self.state}, x: {self.x}, y: {self.y}"

    
class Agent:
    """
    Simulated agent in piece picking experiments

    teach() 
    Reminder, piece pickers live on the TRS tradeoff
    Throughput has to do with network topology
    -> matters when fully connected (i.e. many agents in a field)
    -> doesnt matter when sparsely connected (i.e. many agents in a mine)
    Robustness has to do with link quality (upload can only be as fast as slowest link)
    -> matters when links are heterogenous (i.e. heterogenous swarms)
    -> doesnt matter when links are homogenous (i.e. warehouse with centralized comms)
    Sequentiality matters when the may buffer is short
    -> matters when about to reach last posessed map (i.e. short-range convoy)
    -> doesnt matter when you arent using these maps (i.e. you are creating or repeating on your own maps)
    """
    def __init__(self, rid, x0, y0, radio_quality, policy, role, target_session=None, pol_param=1.0, piece_size=PIECE_SIZE, start_delay=0.0):
        # init
        self.rid = rid
        self.session_id = str(rid) if role == Role.TEACH else None
        self.target_session = target_session # path we are rpeeating to. None if teacher
        self.x = x0
        self.y = y0
        self.radio_quality = radio_quality
        self.policy = policy
        self.role = role
        self.pol_param = pol_param
        self.piece_size = piece_size

        # map arrays
        self.local_maps: list[Submap] = []
        self.remote_maps: dict[str, list[Submap]] = {}
        self.priorities: dict[str, np.ndarray] = {}
        self.download_order: dict[str, list[int]] = {}

        origin = Submap(rid=self.rid, x=x0, y=y0, idx=0, state=Status.FULL_DATA, size_bytes=piece_size)
        self.local_maps.append(origin)
        self.current_submap = origin
        self.start_delay = start_delay

    def teach(self, dx, dy):
        """
        move by dx, dy, create a submap
        """
        # topology = 1, full_data = 2
        self.x += dx
        self.y += dy
        new_submap = Submap(
            rid=self.rid, 
            x=self.x, 
            y=self.y, 
            idx=len(self.local_maps), 
            state=Status.FULL_DATA,
            size_bytes=self.piece_size
        )
        self.local_maps.append(new_submap)
        self.current_submap = new_submap
        return new_submap

    def learn_topology(self, session_id, remote_submaps):
        """
        sim of gossip
        """
        if session_id in self.remote_maps:
            return
        self.remote_maps[session_id] = [
            Submap(rid=s.rid, x=s.x, y=s.y, idx=s.idx, state=Status.TOPOLOGY, size_bytes=self.piece_size)
            for s in remote_submaps
        ]
        self.priorities[session_id] = np.zeros(len(remote_submaps), dtype=int)
        self.download_order[session_id] = []

    def select_leech_target(self, session_id):
        """
        leech from a peer
        """
        pieces = self.remote_maps.get(session_id)
        if not pieces:
            return None
        mask = np.array([p.is_complete for p in pieces])
        if mask.all():
            return None

        # apply piece picking policy
        prio = self.priorities.get(session_id, np.zeros(len(pieces), dtype=int))
        new_prio = np.asarray(self.policy(prio, mask, self.pol_param))
        new_prio = ones_filter(new_prio)
        self.priorities[session_id] = new_prio

        missing_idx = np.where(~mask)[0]
        return int(missing_idx[np.argmax(new_prio[missing_idx])])

    def receive_bytes(self, session_id, piece_idx, num_bytes):
        """
        apply a transfer (based on bandwidth)
        """
        pieces = self.remote_maps.get(session_id)
        if not pieces or piece_idx >= len(pieces):
            return
        piece = pieces[piece_idx]
        was_complete = piece.is_complete
        piece.add_bytes(num_bytes)
        if piece.is_complete and not was_complete:
            self.download_order[session_id].append(piece_idx)

    def has_full_data(self, session_id, idx):
        if session_id == self.session_id:
            return 0 <= idx < len(self.local_maps) and self.local_maps[idx].is_complete
        pieces = self.remote_maps.get(session_id)
        return bool(pieces and 0 <= idx < len(pieces) and pieces[idx].is_complete)

    def topological_move(self, session_id, movement):
        """
        'repeat' along a path
        if next map has full data, proceed, else wait
        movement: +1 forward, -1 backward, 0 stay
        Returns True if successful, else False if have to wait
        """
        pieces = self.remote_maps.get(session_id)
        if not pieces:
            return False
        next_idx = self.current_submap.idx + movement
        if next_idx < 0 or next_idx >= len(pieces):
            return False
        next_submap = pieces[next_idx]
        if next_submap.is_complete:
            self.current_submap = next_submap
            self.x = next_submap.x
            self.y = next_submap.y
            return True
        return False # wait
