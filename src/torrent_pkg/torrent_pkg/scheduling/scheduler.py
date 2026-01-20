import threading

from torrent_pkg.gossip.gossip_interface import GossipInterface
from torrent_pkg.state.piece_state import PieceState

class Scheduler:
    def __init__(self, piece_state: PieceState, gossip: GossipInterface):
        self._lock = threading.Lock()
        self._piece_state = piece_state
        self._gossip = gossip
        self._in_flight = set()

    def choose_next_piece(self):
        """choose next missing piece"""
        with self._lock:
            missing_pieces = self._piece_state.missing()

            if not missing_pieces:
                return None
            
            # who has the missing pieces
            for piece_id in sorted(missing_pieces):
                if piece_id in self._in_flight:
                    continue

                peers = self._gossip.peers_with_piece(piece_id)
                if peers:
                    self._in_flight.add(piece_id)
                    return piece_id
                            
        return None
        