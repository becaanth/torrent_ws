import rclpy
from rclpy.node import Node
from vtr_navigation_msgs.msg import RobotState

import os
import logging

logger = logging.getLogger(__name__)

class RobotStateListener(Node):
    """
    Interface between T&R and Orchestrator
    -> pass T&R messages via callback to Orchestrator
    """
    def __init__(self, on_new_vertex=None):
        super().__init__('robot_state_listener')
        self.on_new_vertex = on_new_vertex
        self.robot_name = os.getenv("ROBOT_NAME")
        print(f"ROBOT_NAME: {self.robot_name}")
        
        self.robot_state_sub = self.create_subscription(
            RobotState, 
            f'{self.robot_name}/vtr/robot_state', 
            self.robot_state_callback,
            10)

    def robot_state_callback(self, robot_state):
        # get id of current vertex were localized to
        current_vtx = robot_state.index
        logger.info(f"localized to vertex {current_vtx}")
        self.on_new_vertex(current_vtx)

def main(args=None):
    rclpy.init(args=args)
    robot_state_listener = RobotStateListener()
    rclpy.spin(robot_state_listener)
    robot_state_listener.destroy_node()
    rclpy.shutdown()

if __name__ == "__main__":
    main()