from torrent_pkg.gossip.gossip_interface import GossipInterface
from torrent_pkg.state.piece_state import PieceState
from torrent_pkg.scheduling.scheduler import Scheduler

import pdb

def visualize_state(piece_state, gossip, scheduler):
    peers = gossip._peer_bitfields           # {peer_id: bitfield}
    peer_ids = sorted(peers.keys())

    max_len = max(
        [len(piece_state.bitfield())] +
        [len(bits) for bits in peers.values()]
    )

    header = "piece | me | " + " | ".join(f"p{pid}" for pid in peer_ids) \
             + " | in_flight | requestable"
    print(header)
    print("-" * len(header))

    for pid in range(max_len):
        me = int(piece_state.have(pid))

        peer_bits = []
        has_peer = False
        for peer_id in peer_ids:
            bit = int(pid < len(peers[peer_id]) and peers[peer_id][pid])
            peer_bits.append(bit)
            has_peer |= bit == 1

        in_flight = "X" if pid in scheduler._in_flight_pieces else ""
        requestable = "yes" if (me == 0 and has_peer and pid not in scheduler._in_flight_pieces) else "no"

        print(
            f"{pid:>5} |  {me} | "
            + " | ".join(f"  {b}" for b in peer_bits)
            + f" |    {in_flight:^7} |    {requestable}"
        )

state = PieceState("../../../../deconstructed/1/woody_convoy")
gossip = GossipInterface(robot_id=1)

scheduler = Scheduler(state, gossip)

# simulate peers
# peer 0 has pieces 0,1,2
gossip.update_peer(0, [1, 1, 1, 0, 0, 0])

# peer 2 has pieces 2,3,4
gossip.update_peer(2, [0, 0, 1, 1, 1, 0])

# peer 3 has pieces 5
gossip.update_peer(3, [0, 0, 0, 0, 0, 1])

p = scheduler.choose_next_piece()
print(f"Next piece: {p}")
visualize_state(state, gossip, scheduler)