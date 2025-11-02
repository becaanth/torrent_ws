import rclpy
from rclpy.node import Node
import os

from std_msgs.msg import UInt8MultiArray, MultiArrayDimension

import pdb

class Registry(Node):
    def __init__(self):
        super().__init__('registry')
        self.publisher_ = self.create_publisher(UInt8MultiArray, '/gossip', 10)
        timer_period = 0.5  # seconds
        self.timer = self.create_timer(timer_period, self.timer_callback)
        folder_path = '/home/asrl/ASRL/vtr3/torrent_ws'
        bag_name = 're_baseline'
        self.chunks_path = f'{folder_path}/deconstructed/{bag_name}'
        self.robot_id = 0 # each chunk gets a Uint8, bitfield based on robot id

    def timer_callback(self):
        msg = UInt8MultiArray()
        # Suppose chunk C0 is available on R1 and R3 of a 4-robot fleet
        # Bit 0 = R0, Bit 1 = R1, Bit 2 = R2, Bit 3 = R3
        chunks = self.find_chunks()
        my_chunks = [0,1,2,3,5,8,113]

        # Initialize a byte per chunk, all zeros
        bitfield_per_chunk = [0] * len(chunks)

        # Set the bit corresponding to this robot for the chunks it has
        for chunk in my_chunks:
            bitfield_per_chunk[chunk] |= (1 << self.robot_id)

        msg.data = bitfield_per_chunk  # R1 and R3 have the chunk
        self.publisher_.publish(msg)
        
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