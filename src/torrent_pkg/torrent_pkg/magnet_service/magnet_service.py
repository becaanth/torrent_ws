import rclpy
from rclpy.node import Node

from torrent_pkg.torrent.torrent_manager import TorrentManager
from torrent_msgs.srv import GetMagnetURI

class MagnetService:
    def __init__(self, node: Node, torrent_manager: TorrentManager):
        self._node = node
        self._tm = torrent_manager

        self._srv = node.create_service(
            GetMagnetURI,
            f"robot_{node.robot_id}/magnet_service",
            self._handle_request
        )

    def _handle_request(self, request, response):
        piece_id = request.piece_id

        try:
            magnet = self._tm.seed_piece(piece_id)
            response.magnet_uri = magnet
            self._node.get_logger().info(
                f"[magnet] served piece {piece_id}"
            )
        except FileNotFoundError:
            response.magnet_uri = ""
            self._node.get_logger().warn(
                f"[magnet] piece {piece_id} not found"
            )
        except Exception as e:
            response.magnet_uri = ""
            self._node.get_logger().error(
                f"[magnet] error seeding {piece_id}: {e}"
            )

        return response