import rclpy
from rclpy.node import Node
import numpy as np
import os

from std_msgs.msg import UInt8MultiArray, MultiArrayDimension
from torrent_msgs.msg import SubmapRegistry

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
    return bitfield


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

        self.gossip_most_chunks = 0
        os.makedirs(self.chunks_path, exist_ok=True)

        timer_period = 1  # seconds
        self.timer = self.create_timer(timer_period, self.timer_callback)

        self.publisher_ = self.create_publisher(SubmapRegistry, f'/robot_{self.robot_id}/gossip', 10)
        self.subscribers = []
        for robot_id in range(num_robots):
            topic_name = f'/robot_{robot_id}/gossip'
            if robot_id != self.robot_id:
                sub = self.create_subscription(
                    SubmapRegistry,
                    topic_name,
                    lambda msg, rid=robot_id: self.gossip_callback(msg, rid),
                    10
                )
                self.subscribers.append(sub)

        self.subscription = self.create_subscription(SubmapRegistry,'/robot',self.gossip_callback,10)
        
    def timer_callback(self):
        msg = SubmapRegistry()
        # Suppose chunk C0 is available on R1 and R3 of a 4-robot fleet
        # Bit 0 = R0, Bit 1 = R1, Bit 2 = R2, Bit 3 = R3
        chunks = self.find_chunks()
        msg.num_submaps = len(chunks)
        if len(chunks) > self.gossip_most_chunks:
            self.gossip_most_chunks = len(chunks)

        # Initialize a byte per chunk, all zeros
        bitfield = [0] * (len(chunks) + len(chunks)%8)
        for i, chunk in enumerate(chunks):
            bitfield[chunk] = 1

        # Convert bits to bytes
        encoded = bits_to_string(bitfield)

        print(f'num submaps: {msg.num_submaps}, bitfield: {encoded}')
        msg.possessed_submaps = encoded

        self.publisher_.publish(msg)

    def gossip_callback(self, msg, robot_id):
        self.get_logger().info(f"Received gossip from robot {robot_id}: {msg.num_submaps} submaps")
        # here we can compare submaps we have with submaps from other robots
        if self.gossip_most_chunks < msg.num_submaps:
            self.gossip_most_chunks = msg.num_submaps # max num submaps equivalent to most heard

        # what chunks does other robot have?
        their_chunks = string_to_bits(msg.possessed_submaps)

        # what chunks do i have?
        my_chunks = self.find_chunks()

        # make arrs compatible

        # pdb.set_trace()
        
    def find_chunks(self):
        # List all .db3 chunk files in sorted order
        db_files = [f for f in os.listdir(self.chunks_path) if f.endswith('.db3')]
        chunks = sorted([int(f.split('.')[0]) for f in db_files])
        return chunks


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