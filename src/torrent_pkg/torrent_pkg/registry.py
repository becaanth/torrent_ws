import rclpy
from rclpy.node import Node
import libtorrent as lt
import numpy as np
import os
import time
import socket

from std_msgs.msg import String
from torrent_msgs.msg import SubmapRegistry
from torrent_msgs.srv import GetMagnetURI
import pdb

def bits_to_string(bitfield):
    chars = []
    for i in range(0, len(bitfield), 8):
        byte = bitfield[i:i+8]
        byte_value = int(''.join(str(b) for b in byte), 2)
        chars.append(chr(byte_value))
    return ''.join(chars)

def string_to_bits(s):
    bitfield = []
    for c in s:
        byte = ord(c)
        bitfield.extend([(byte >> bit) & 1 for bit in range(7, -1, -1)])
    return np.array(bitfield)

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('10.255.255.255', 1))
        return s.getsockname()[0]
    except:
        return '127.0.0.1'
    finally:
        s.close()

class Registry(Node):
    def __init__(self, num_robots=4):
        super().__init__('registry')
        # Declare the parameter and provide a default value
        self.declare_parameter('robot_id', 0)
        self.robot_id = self.get_parameter('robot_id').value
        print(f'ROBOT ID: {self.robot_id}')
        
        folder_path = '/home/asrl/ASRL/vtr3/torrent_ws'
        bag_name = 're_baseline'
        self.chunks_path = f'{folder_path}/deconstructed/{self.robot_id}/{bag_name}'
        os.makedirs(self.chunks_path, exist_ok=True)

        timer_period = 1  # seconds
        self.timer = self.create_timer(timer_period, self.timer_callback)

        self.gossip_most_chunks = 1

        # gossip this robots current chunks
        self.pub_registry = self.create_publisher(SubmapRegistry, f'/robot_{self.robot_id}/gossip', 10)

        # subscribe to fleet gossip
        # NOTE: we are using gossip for peer discovery, this is proof of concept
        self.subscribers = []
        self.robot_registries = {} # dictionary of which robots have what chunks
        for robot_id in range(num_robots):
            topic_name = f'/robot_{robot_id}/gossip'
            if robot_id != self.robot_id:
                self.robot_registries[robot_id] = []
                sub = self.create_subscription(
                    SubmapRegistry,
                    topic_name,
                    lambda msg, rid=robot_id: self.gossip_callback(msg, rid),
                    10
                )
                self.subscribers.append(sub)
        self.subscription = self.create_subscription(SubmapRegistry,'/robot',self.gossip_callback,10)

        # --- Magnet URI publisher (optional broadcast) ---
        self.pub_magnetURI = self.create_publisher(String, f'/robot_{self.robot_id}/magnet_uri', 10)

        # --- Magnet URI service ---
        service_name = f'/robot_{self.robot_id}/magnet_service'
        self.srv_magnet = self.create_service(
            GetMagnetURI,
            service_name,
            self.magnet_service_callback
        )
        print(f'Magnet URI service running on: {service_name}')

    def timer_callback(self):
        msg = SubmapRegistry()
        chunks = self.find_chunks()
        msg.num_submaps = len(chunks)
        if len(chunks) > self.gossip_most_chunks:
            self.gossip_most_chunks = len(chunks)

        # Initialize a byte per chunk, all zeros
        bitfield = [0] * (len(chunks) + len(chunks)%8)
        for i, chunk in enumerate(chunks):
            bitfield[int(chunk)] = 1

        # Convert bits to bytes
        encoded = bits_to_string(bitfield)

        print(f'num submaps: {msg.num_submaps}, bitfield: {encoded}')
        msg.possessed_submaps = encoded

        self.pub_registry.publish(msg)

    def gossip_callback(self, msg, robot_id):
        self.get_logger().info(f"Received gossip from robot {robot_id}: {msg.num_submaps} submaps")
        # here we can compare submaps we have with submaps from other robots
        # what chunks does other robot have?
        self.robot_registries[robot_id] = string_to_bits(msg.possessed_submaps)

        self.update_registries()
        # what chunks do i have?
        my_chunks = self.find_chunks()

        # what chunk do i need? go from start to end
        count = 0
        found = False

        while count < len(my_chunks) and not found:
            if my_chunks[count] == 0:
                # who has the chunk we need?
                for rid in self.robot_registries.keys():
                    try:
                        if self.robot_registries[rid][count] == 1:
                            # this robot has the chunk we need
                            # ask for magnet URI
                            print(f'robot{rid} has next chunk {count}, asking for magnet URI')
                            self.request_magnet_URI(rid, count)
                            found = True  # stop the outer while loop
                            break          # stop checking other robots for this chunk
                    except KeyError:
                        continue
            count += 1

    def magnet_service_callback(self):
        # TODO: HANDLE LIBTORRENT FOR SETTING UP MAGNET URI
        a=1

    def request_magnet_URI(self, rid, chunk_id):
        client = self.create_client(GetMagnetURI, f'/robot_{rid}/get_magnet')
        while not client.wait_for_service(timeout_sec=1.0):
            self.get_logger().info(f'Service not available yet: robot {rid}')
        req = GetMagnetURI.Request()
        req.chunk_id = chunk_id
        future = client.call_async(req)
        return future
        
    def find_chunks(self):
        # List all .db3 chunk files in sorted order
        db_files = [f for f in os.listdir(self.chunks_path) if f.endswith('.db3')]
        chunks = sorted([int(f.split('.')[0]) for f in db_files])
        max_len = max(len(bits) for bits in self.robot_registries.values())
        chunks = np.concatenate([chunks, np.zeros(max_len - len(chunks), dtype=np.uint8)])
        return np.array(chunks)
    
    def update_registries(self):
        """Combine all known robot registries into a global view."""
        if not self.robot_registries:
            return

        # Find the max bitfield length (some robots may have more chunks)
        max_len = max(len(bits) for bits in self.robot_registries.values())

        # Pad bitfields to equal length
        aligned = []
        for rid, bits in self.robot_registries.items():
            if len(bits) < max_len:
                padded = np.concatenate([bits, np.zeros(max_len - len(bits), dtype=np.uint8)])
            else:
                padded = bits
            aligned.append(padded)

        # Stack into a matrix: rows = robots, columns = chunks
        # registry_matrix = np.vstack(aligned)

        self.get_logger().info(
            f"Updated global registry: {max_len} total chunks across fleet"
        )


def main(args=None):
    rclpy.init(args=args)

    registry = Registry()

    rclpy.spin(registry)

    # Destroy the node explicitly
    # (optional - otherwise it will be done automatically
    # when the garbage collector destroys the node object)
    registry.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()