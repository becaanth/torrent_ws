# test_gossip_interface.py
from torrent_pkg.gossip.gossip_interface import GossipInterface


def main():
    gossip = GossipInterface(robot_id=1)

    # Simulate incoming gossip from peers
    gossip.update_peer(0, [True, True, True, True])     # robot 0 has all pieces
    gossip.update_peer(2, [True, False, False, False])  # robot 2 has only piece 0
    gossip.update_peer(3, [False, False, False, False])

    print("Known peers:", gossip.peers())
    print("Snapshot:", gossip.snapshot())

    # Query availability
    availability = gossip.availability()
    print("Availability:", availability)

    # Who has each piece?
    for piece_id in range(4):
        peers = gossip.peers_with_piece(piece_id)
        print(f"Peers with piece {piece_id}: {peers}")

    # Sanity checks
    assert gossip.peers_with_piece(0) == [0, 2]
    assert gossip.peers_with_piece(1) == [0]
    assert gossip.peers_with_piece(2) == [0]
    assert gossip.peers_with_piece(3) == [0]

    print("\nAll tests passed ✔")


if __name__ == "__main__":
    main()
