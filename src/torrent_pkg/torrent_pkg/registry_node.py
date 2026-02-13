"""
Docstring for torrent_ws.src.torrent_pkg.torrent_pkg.torrent_agent

Torrent and Repeat Convoying base node
-> agents must be able to view/update/broadcast their piece possession
-> agents must be able to seed/leech piece transfers with libtorrent

Levels of abstraction
PieceState -> interfaces with filesystem
GossipInterface -> link PieceState with gossip topic
Scheduler -> what piece is next and from whom
 -> read PieceState, GossipInterface
MagnetService -> asks agents for pieces
 -> read PieceState, queries TorrentManager
TorrentManager -> execute torrents
"""


import rclpy
from rclpy.node import Node
from std_msgs.msg import String

import os
import threading

from torrent_pkg.state.piece_state import PieceState
from torrent_pkg.gossip.gossip_interface import GossipInterface
from torrent_pkg.scheduling.scheduler import Scheduler
from torrent_pkg.magnet_service.magnet_service import MagnetService
from torrent_pkg.torrent.torrent_manager import TorrentManager
from torrent_pkg.utils.utils import *
from torrent_msgs.srv import GetMagnetURI
from torrent_msgs.msg import SubmapRegistry

class RegistryNode(Node):
    def __init__(self, num_robots: int = 4):
        super().__init__('registry_node')

        self.declare_parameter('robot_id', 0)
        self.robot_id = self.get_parameter('robot_id').value

        self.get_logger().info(
            f'RegistryNode started (robot_id={self.robot_id})'
        )

        self.declare_parameter('base_path', '/home/asrl/ASRL/vtr3/torrent_ws')
        base_path = self.get_parameter('base_path').value

        self.declare_parameter('bag_name', '')
        bag_name = self.get_parameter('bag_name').value
        
        self.pieces_path = (
            f'{base_path}/deconstructed/{self.robot_id}/{bag_name}'
        )
        print(f'[init]: reading from pieces path {self.pieces_path}')
        os.makedirs(self.pieces_path, exist_ok=True)

        # Core State (interface with filesystem)
        self.piece_state = PieceState(self.pieces_path)

        # Transport via Torrent
        self.torrent_manager = TorrentManager(self.pieces_path)

        # Gossip
        self.gossip = GossipInterface(robot_id=self.robot_id)
        self._gossip_pub = self.create_publisher(
            SubmapRegistry,
            f"/robot_{self.robot_id}/gossip",
            10
        )
        self._gossip_subs = []
        for peer_id in range(num_robots):
            if peer_id == self.robot_id:
                continue

            sub = self.create_subscription(
                SubmapRegistry,
                f"/robot_{peer_id}/gossip",
                lambda msg, pid=peer_id: self._on_gossip(msg, pid),
                10
            )
            self._gossip_subs.append(sub)

        # Scheduler
        self.scheduler = Scheduler(
            piece_state=self.piece_state,
            gossip=self.gossip
        )

        # Magnet Service
        self.magnet_service = MagnetService(
            node=self,
            torrent_manager=self.torrent_manager
        )

        # Execution loop
        self._tick_timer = self.create_timer(
            0.5, self._tick
        )

        # Logger loop
        self._log_timer = self.create_timer(
            2.0, self._log_status
        )


    def destroy_node(self):
        self.torrent_manager.shutdown()
        super().destroy_node()
    

    def _log_status(self):
        self.get_logger().info(
            f"[status] have={self.piece_state.bitfield()} "
            # f"in_flight={self.scheduler._in_flight()}"
        )
        self.torrent_manager.visualize()

    def _publish_gossip(self):
        msg = SubmapRegistry()
        bits = self.piece_state.bitfield()

        msg.num_submaps = sum(bits)
        msg.possessed_submaps = bits_to_string(bits)
        self._gossip_pub.publish(msg)

    def _on_gossip(self, msg, peer_id):
        # self.get_logger().info(f"[gossip] gossip from {peer_id}: {msg.possessed_submaps}")
        bitfield = string_to_bits(msg.possessed_submaps)
        self.gossip.update_peer(peer_id, bitfield)
        self.piece_state.ensure_capacity(msg.num_submaps)

    def _tick(self):
        """
        Periodic control loop to 1) publish gossip, 2) choose next piece, 3) request magnet, 4) start download
        """
        # self.get_logger().info(
        #         f"gossip peers={self.gossip.peers()} snapshot={self.gossip.snapshot()}"
        # #    f"bitfield={self.piece_state.bitfield()} missing={self.piece_state.missing()}"
        # )

        # publish gossip
        self._publish_gossip()

        decision = self.scheduler.choose_next_piece()
        # self.get_logger().info(
        #     f"[scheduler] decision is {decision}"
        # )
        if decision is None:    
            return
        
        piece_id = decision
        peers = self.gossip.peers_with_piece(piece_id)
        peer_id = peers[0]
        # self.get_logger().info(
        #     f"[scheduler] requesting piece {piece_id} from robot {peer_id}"
        # )

        self._request_magnet(peer_id, piece_id)

    def _request_magnet(self, peer_id: int, piece_id: int):
        service_name = f"/robot_{peer_id}/magnet_service"
        client = self.create_client(
            GetMagnetURI,
            service_name
        )

        if not client.service_is_ready():
            self.get_logger().warn(
                f"[magnet] service not ready: {service_name}"
            )
            return
        
        req = GetMagnetURI.Request()
        req.piece_id = piece_id

        future = client.call_async(req)
        future.add_done_callback(
            lambda f: self._on_magnet_response(f, piece_id)
        )

        self.scheduler._in_flight.add(piece_id)

    def _on_magnet_response(self, future, piece_id: int):
        try:
            resp = future.result()
        except Exception as e:
            self.get_logger().warn(f"[magnet] request failed: {e}")
            self.scheduler._in_flight.remove(piece_id)
            return
    
        if not resp.magnet_uri:
            self.get_logger().warn(
                f"[magnet] empty response for piece {piece_id}"
            )
            self.scheduler._in_flight.remove(piece_id)
            return

        self.torrent_manager.download_piece(
            piece_id, resp.magnet_uri
        )

def main(args=None):
    rclpy.init(args=args)

    registry_node = RegistryNode()

    rclpy.spin(registry_node)

    registry_node.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()