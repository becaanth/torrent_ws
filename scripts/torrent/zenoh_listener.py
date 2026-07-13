import zenoh
from torrent_utils import *
from queue import Queue

message_queue = Queue()

def on_sample(sample):
    print('[Peer]: Received Zenoh message')
    message_queue.put(sample)

if __name__ == "__main__":
    with open(f'torrent/seeder_params.json', "r") as f:
        params = json.load(f)

    robot_id = 0
    container = params['container']
    my_ip, z_cfg = unpack_device(params, robot_id)
    z_cfg.insert_json5("mode", '"client"')
    z_ses = zenoh.open(z_cfg)
    sub = z_ses.declare_subscriber("mutable_items/**", on_sample)

    try:
        while True:
            # Check for new messages (non-blocking)
            if not message_queue.empty():
                sample = message_queue.get()

                mutable_item = on_mutable_item(sample)
                print(f"[test]: mutable item received \n {mutable_to_string(mutable_item)}")
    except KeyboardInterrupt:
        print("Keyboard Interruption")