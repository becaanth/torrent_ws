import threading
from typing import Dict, List

class GossipInterface:
    def __init__(self, robot_id: int):
        self._lock = threading.Lock()
        self._robot_id = robot_id
        self._peer_bitfields: Dict[int, List[bool]] = {}


    def update_peer(self, peer_id: int, bitfield: List[bool]):
        """update from incoming gossip"""
        if peer_id == self._robot_id:
            return 
        
        with self._lock:
            self._peer_bitfields[peer_id] = list(bitfield)

    def peers(self) -> List[int]:
        """return a list of known peer ids"""
        with self._lock:
            return list(self._peer_bitfields.keys())
        
    def peers_with_piece(self, piece_id: int) -> List[int]:
        """return peers that claim to have a piece"""
        with self._lock:
            peers = []
            for peer_id, bf in self._peer_bitfields.items():
                if piece_id < len(bf) and bf[piece_id]:
                    peers.append(peer_id)
            return peers
            
    def availability(self) -> Dict[int,int]:
        """piece_id -> number of peers who have it"""
        with self._lock:
            counts = {}
            for bf in self._peer_bitfields.values():
                for i, have in enumerate(bf):
                    if have:
                        counts[i] = counts.get(i,0)+1
            return counts
            
    def snapshot(self) -> Dict[int, List[bool]]:
        """Debugging helper: deep copy of peer state."""
        with self._lock:
            return {pid: list(bf) for pid, bf in self._peer_bitfields.items()}