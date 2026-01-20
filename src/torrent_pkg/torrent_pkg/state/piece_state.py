import os
import threading

class PieceState:
    """
    Interact with filesystem to get piece possession
    """
    def __init__(self, pieces_path: str):
        self._lock = threading.Lock()
        self._pieces_path = pieces_path
        self._have = self._scan_filesystem()
        print(self._have)

    def _scan_filesystem(self):
        """
        Scan pieces_path for *.db3 files and build a boolean list.
        This runs ONCE at startup.
        """
        piece_ids = []

        for fname in os.listdir(self._pieces_path):
            if fname.endswith(".db3"):
                try:
                    piece_id = int(fname.split(".")[0])
                    piece_ids.append(piece_id)
                except ValueError:
                    continue  # ignore malformed filenames

        if not piece_ids:
            return []

        max_piece = max(piece_ids)
        have = [False] * (max_piece + 1)

        for pid in piece_ids:
            have[pid] = True

        return have

    def have(self, piece_id) -> bool:
        # do I have this piece?
        with self._lock:
            if piece_id >= len(self._have):
                return False
            return self._have[piece_id]
        
    def missing(self) -> list[int]:
        # what pieces do I not have yet? (Scheduler)
        with self._lock:
            return [i for i, have in enumerate(self._have) if not have]

    def bitfield(self) -> list[bool]:
        # prepare for gossip (GossipInterface)
        with self._lock:
            return list(self._have)

    def mark_complete(self, piece_id):
        # this piece is locally complete
        with self._lock:
            if piece_id >= len(self._have):
                # grow the list if needed
                self._have.extend([False] * (piece_id + 1 - len(self._have)))

            self._have[piece_id] = True

    def ensure_capacity(self, n: int):
        with self._lock:
            if len(self._have) < n:
                self._have.extend([False] * (n - len(self._have)))
