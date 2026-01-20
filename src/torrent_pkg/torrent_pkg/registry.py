import rclpy
from rclpy.node import Node
import libtorrent as lt
import numpy as np
import os
import time
import socket
import threading
from queue import Queue

from std_msgs.msg import String
from torrent_msgs.msg import SubmapRegistry
from torrent_msgs.srv import GetMagnetURI
import pdb

def bits_to_string(bitfield):
    """Pack bitfield into a string message type"""
    chars = []
    for i in range(0, len(bitfield), 8):
        byte = bitfield[i:i+8]
        byte_value = int(''.join(str(b) for b in byte), 2)
        chars.append(chr(byte_value))
    return ''.join(chars)

def string_to_bits(s):
    """Unpack string into bitfield"""
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

def seed_thread(file_path, s_q, l_q):
    """
    Thread to host the torrent session
    
    :param file_path: point to this agents pieces
    :param q: pieces to seed
    """
    print('seed thread')

    ses = lt.session()
    ses.listen_on(6881, 6891)
    ses.start_dht()
    ses.start_lsd()
    fs = lt.file_storage()

    while True:
        time.sleep(5)
        # with s_q.mutex:
        #     print('seed', list(s_q.queue))

        if not s_q.empty():
            piece_id = s_q.get() # first item
            lt.add_files(fs, f'{file_path}/{piece_id}.db3')
            t = lt.create_torrent(fs)
            lt.set_piece_hashes(t, os.path.dirname(file_path))
            torrent = t.generate()

            torrent_path = file_path + '.torrent'
            with open(torrent_path, "wb") as f:
                f.write(lt.bencode(torrent))

            ti = lt.torrent_info(torrent)
            params = {
                'save_path': os.path.dirname(file_path),
                'ti': ti
            }

            h = ses.add_torrent(params)
            magnet_uri = lt.make_magnet_uri(ti)
            l_q.put(magnet_uri)

            print(f"[+] Seeding started for: {h.name()}")
            print(f"[+] Magnet URI:\n{magnet_uri}")

            # start seeding
            while h.status().progress < 1.0:
                s = h.status()
                print(f"[SEEDING] Peers: {s.num_peers} - Upload: {s.upload_rate / 1000:.2f} kB/s")
                # Print list of connected peers
                peer_info = h.get_peer_info()
                print(f"    ↪ Connected peers ({len(peer_info)}):")
                for peer in peer_info:
                    print(f"        - {peer.ip} ({peer.client})")

def leech_thread(file_path, s_q, l_q):
    """
    Thread to download magnet_links
    
    :param file_path: point to this agents pieces
    :param leech_queue: magnet queues to download
    """
    print('leech thread')
    ses = lt.session()
    ses.listen_on(6881, 6891)
    ses.start_dht()
    ses.start_lsd()

    params = {
    'save_path': file_path,
    'storage_mode': lt.storage_mode_t.storage_mode_sparse,
    }

    while True:
        time.sleep(5)
        # with s_q.mutex:
        #     print('leech', list(l_q.queue))

        if not l_q.empty():
            magnet_uri = l_q.get()
            h = lt.add_magnet_uri(ses, magnet_uri, params)
            while not h.has_metadata():
                time.sleep(1)
            # print("[+] Downloading...")

            while not h.status().progess < 1.0:
                s = h.status()
                print(f"[DOWNLOADING] {s.progress * 100:.2f}% - "
                    f"Peers: {s.num_peers} - "
                    f"Download: {s.download_rate / 1000:.2f} kB/s")
                time.sleep(2)
                # Print list of connected peers
                peer_info = h.get_peer_info()
                print(f"    ↪ Connected peers ({len(peer_info)}):")
                for peer in peer_info:
                    print(f"        - {peer.ip} ({peer.client})")
                
                print("[+] Download complete.")
                print(f"[+] File saved to: {file_path}/{h.name()}")

def torrent_manager(pieces_path, seed_q, leech_q, stop_event):
    import libtorrent as lt
    import time
    import os
    import queue

    print("[torrent] manager started")
    ses = lt.session()
    ses.listen_on(6881, 6891)
    ses.start_dht()
    ses.start_lsd()

    active_handles = []

    while not stop_event.is_set():
        # seed requests
        try:
            piece_id = seed_q.get_nowait()
        except queue.Empty:
            piece_id = None
        
        if piece_id is not None:
            file_path = os.path.join(pieces_path, f"{piece_id}.db3")
            if os.path.exists(file_path):
                fs = lt.file_storage()
                lt.add_files(fs, file_path)
                t = lt.create_torrent(fs)
                lt.set_piece_hashes(t, os.path.dirname(file_path))
                ti = lt.torrent_info(t.generate())

                h = ses.add_torrent({
                    'ti' : ti,
                    'save_path' : os.path.dirname(file_path),
                })
                active_handles.append(h)

                magnet = lt.make_magnet_uri(ti)
                leech_q.put(magnet)

                print(f"[torrent] seeding piece {piece_id}")
            seed_q.task_done()

        # leech requests
        try:
            magent = leech_q.get_nowait()
        except queue.Empty:
            magnet = None

        if magnet is not None:
            h = lt.add_magnet_uri(
                ses,
                magnet,
                {'save_path' : pieces_path}
            )
            active_handles.append(h)
            leech_q.task_done()

        # poll torrent status
        for h in list(active_handles):
            if not h.is_valid():
                active_handles.remove()
                continue

            s = h.status()
            if s.progress >= 1.0:
                print(f"[torrent] complete: {h.name()}")
                active_handles.remove(h)

        time.sleep(0.5)

    print(f"[torrent] manager exiting")


class Registry(Node):
    def __init__(self, num_robots=4):
        super().__init__('registry')
        # Declare the parameter and provide a default value
        self.declare_parameter('robot_id', 0)
        self.robot_id = self.get_parameter('robot_id').value
        print(f'ROBOT ID: {self.robot_id}')
        
        folder_path = '/home/asrl/ASRL/vtr3/torrent_ws'
        bag_name = 'woody_convoy'
        self.pieces_path = f'{folder_path}/deconstructed/{self.robot_id}/{bag_name}'
        os.makedirs(self.pieces_path, exist_ok=True)

        # init threading
        self.seed_queue = Queue()
        self.leech_queue = Queue()
        self.stop_event = threading.Event()

        # self.seed_thread = threading.Thread(
        #     target=seed_thread,
        #     args=(self.pieces_path, self.seed_queue, self.leech_queue),
        #     daemon=True
        # )
        # self.leech_thread = threading.Thread(
        #     target=leech_thread,
        #     args=(self.pieces_path, self.seed_queue, self.leech_queue),
        #     daemon=True
        # )
        # self.seed_thread.start()
        # self.leech_thread.start()

        self.torrent_thread = threading.Thread(
            target=torrent_manager,
            args=(
                self.pieces_path,
                self.seed_queue,
                self.leech_queue,
                self.stop_event
            ),
            daemon=True
        )
        self.torrent_thread.start()

        timer_period = 1  # seconds
        self.timer = self.create_timer(timer_period, self.timer_callback)
        self.async_logger = self.create_timer(timer_period, self.logger_callback)

        # largest number of pieces in the possession of any agent
        self.gossip_most_pieces = 1
        # gossip this robots current pieces (as a string)
        self.pub_registry = self.create_publisher(SubmapRegistry, f'/robot_{self.robot_id}/gossip', 10)

        # subscribe to fleet gossip
        # NOTE: we are using gossip for peer discovery, this is proof of concept
        self.subscribers = []
        self.robot_registries = {} # dictionary of which robots have what pieces
        for robot_id in range(num_robots):
            topic_name = f'/robot_{robot_id}/gossip'
            if robot_id != self.robot_id:
                self.robot_registries[robot_id] = []
                # subscribe to gossip topics
                sub = self.create_subscription(
                    SubmapRegistry,
                    topic_name,
                    lambda msg, robot_id=robot_id: self.gossip_callback(msg, robot_id),
                    10
                )
                self.subscribers.append(sub)

        # Magnet URI publisher
        self.pub_magnetURI = self.create_publisher(String, f'/robot_{self.robot_id}/magnet_uri', 10)

        # Magnet URI service
        service_name = f'/robot_{self.robot_id}/magnet_service'
        self.srv_magnet = self.create_service(
            GetMagnetURI,
            service_name,
            self.magnet_service_callback
        )
        print(f'Magnet URI service running on: {service_name}')


    def logger_callback(self):
        self.get_logger().info("[logger]: "
        f"seed_q={self.seed_queue.qsize()} "
        f"leech_q={self.leech_queue.qsize()}"
    )

    def timer_callback(self):
        """
        Broadcast the pieces in this agent's possession
        """
        msg = SubmapRegistry()
        pieces = self.find_pieces()
        msg.num_submaps = len(pieces)
        if len(pieces) > self.gossip_most_pieces:
            self.gossip_most_pieces = len(pieces)

        # Initialize a byte per piece, all zeros
        bitfield = [0] * (len(pieces) + len(pieces)%8)
        for i, piece in enumerate(pieces):
            bitfield[int(piece)] = 1

        bitstring = bits_to_string(bitfield)
        # print(f'num submaps: {msg.num_submaps}, bitfield: {bitstring}')
        msg.possessed_submaps = bitstring

        self.pub_registry.publish(msg)

    def gossip_callback(self, msg, robot_id):
        """Based on gossip, update registries"""
        # self.get_logger().info(f"Received gossip from robot {robot_id}: {msg.num_submaps} submaps")
        self.robot_registries[robot_id] = string_to_bits(msg.possessed_submaps)
        self.update_registries()
        my_pieces = self.find_pieces()

        # what piece does this agent need? go from start to end
        count = 0
        found = False

        while count < len(my_pieces) and not found:
            if my_pieces[count] == 0:
                for robot_id in self.robot_registries.keys():
                    try:
                        if self.robot_registries[robot_id][count] == 1:
                            # ask for magnet URI
                            # print(f'robot{robot_id} has next piece {count}, asking for magnet URI')
                            self.request_magnet_URI(robot_id, count)
                            found = True  # stop the outer while loop
                            break         # stop checking other robots for this piece
                    except KeyError:
                        continue
            count += 1

    def magnet_service_callback(self, request, response):
        # TODO: HANDLE LIBTORRENT FOR SETTING UP MAGNET URI
        print(f'servicing {request}')
        self.get_logger().info(f'incoming request for {request.piece_id}')
        response.magnet_uri = 'working_on_it'
        return response

    def request_magnet_URI(self, robot_id, piece_id):
        print(f'client requesting {piece_id}')
        client = self.create_client(GetMagnetURI, f'/robot_{robot_id}/magnet_service')
        while not client.wait_for_service(timeout_sec=1.0):
            self.get_logger().info(f'Service not available yet: robot {robot_id}')
        req = GetMagnetURI.Request()
        req.piece_id = piece_id
        self.seed_queue.put(piece_id)
        future = client.call_async(req)
        return future
        
    def find_pieces(self):
        """Find what *.db3 files this agent has"""
        db_files = [f for f in os.listdir(self.pieces_path) if f.endswith('.db3')]
        pieces = sorted([int(f.split('.')[0]) for f in db_files])
        # find the most amount of pieces in any one robots possession
        max_len = max(len(bits) for bits in self.robot_registries.values())
        if max_len < len(pieces):
            max_len = len(pieces)

        pieces = np.concatenate([pieces, np.zeros(max_len - len(pieces), dtype=np.uint8)])
        return np.array(pieces)
    
    def update_registries(self):
        """Combine all known robot registries into a global view."""
        if not self.robot_registries:
            return

        # Find the max bitfield length (some robots may have more pieces)
        max_len = max(len(bits) for bits in self.robot_registries.values())

        # Pad bitfields to equal length
        aligned = []
        for robot_id, bits in self.robot_registries.items():
            if len(bits) < max_len:
                padded = np.concatenate([bits, np.zeros(max_len - len(bits), dtype=np.uint8)])
            else:
                padded = bits
            aligned.append(padded)

        # self.get_logger().info(
        #     f"Updated global registry: {max_len} total pieces across fleet"
        # )

def main(args=None):
    rclpy.init(args=args)
    registry = Registry()

    rclpy.spin(registry)
    registry.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()