import zenoh
import msgpack
import logging
import time
from threading import Lock

from torrent.torrent_utils import unpack_device

logger = logging.getLogger(__name__)

class ZenohGossiper:
    """
    Handle mutable item Zenoh pub/sub between agents in the swarm
    This is an implementation of a discovery for mutable torrents on MANETs
    A mutable item contains:
    - the robot id of the source of the pieces
    - the infohash of the torrent session
    - a (monotonically) increasing sequence number for version control

    Consensus is enforced because 
    1) only the source of the pieces can increment the sequence
    2) T&R maps are append-only, so stale hashes can still seed useful info among disconnected peers
    """
    def __init__(self, params : dict, robot_id : int, poll_hz : float = 1.0,
        on_new_item=None, on_item_updated=None):
        # setup
        self.container = params['container']
        self.router = params['router']
        self.robot_id = robot_id

        # zenoh objects
        self.my_ip, z_cfg = unpack_device(params, self.robot_id, zenoh_port=5203)
        z_cfg.insert_json5("open/return_conditions/connect_scouted", "false")
        self.z_ses = zenoh.open(z_cfg)
        self.sub = self.z_ses.declare_subscriber("mutable_items/**", self._on_sample)
        logging.info("Zenoh session init")

        self.last_sample = None
        self._pending_lock = Lock()
        self._pending_items = {} # robot_id -> latest not processed mi

        self.mutable_items = {} # robot_id -> latest known mi
        self.max_seq_seen = {} # robot_id -> highest seq observed

        # callbacks to seeder and peer
        self.on_new_item = on_new_item
        self.on_item_updated = on_item_updated

        # etc
        self.poll_hz = poll_hz

    def run(self):
        logging.info(f"polling at {self.poll_hz} Hz (Ctrl-C to stop)")
        try: 
            while True:
                self._drain_pending()
                self._rebroadcast_known_items()
                time.sleep(1.0 / self.poll_hz)
        except KeyboardInterrupt:
            logging.info("\nstopped.")
        finally:
            self.z_ses.close()

    def _publish_item(self, mutable_item : dict):
        """
        called by orchestrator when the seeder has a new snapshot that needs to be reannounced
        """
        item = dict(mutable_item)
        item['my_ip'] = self.my_ip
        self.mutable_items[item['robot_id']] = item
        self.max_seq_seen[item['robot_id']] = item['seq']
        self._put(item)

    def _put(self, mutable_item : dict):
        payload = msgpack.packb(mutable_item, use_bin_type=True)
        logging.debug(f"putting zenoh item for robot_id {mutable_item['robot_id']} seq {mutable_item['seq']}")
        self.z_ses.put(f"mutable_items/{mutable_item['robot_id']}", payload)

    def _forward_gossip(self, mutable_item):
        """gossip
        instead of spawning a new seeder, creating conflicts at the snapshot level, use libtorrent to handle multi-seeding
        """
        relay_mi = dict(mutable_item)
        relay_mi['my_ip'] = self.my_ip
        logging.debug(f"forwarding gossip for robot_id {relay_mi['robot_id']} seq {relay_mi['seq']} via {self.my_ip}")
        self._put(relay_mi)

    def _rebroadcast_known_items(self):
        """
        periodically re-announce everything currently known
        """
        logging.debug(f"rebroadcasting known items")
        for robot_id, mutable_item in self.mutable_items.items():
            self._forward_gossip(mutable_item)

    def sample_to_mutable_item(self, sample):
        # Update mutable item
        zenoh_item = msgpack.unpackb(bytes(sample.payload), raw=False)
        mutable_item = {
            'pubkey' : zenoh_item.get('pubkey'),
            'robot_id' : zenoh_item.get('robot_id'),
            'seq' : zenoh_item.get('seq'),
            'infohash' : zenoh_item.get('infohash'),
            'my_ip' : zenoh_item.get('my_ip')
        }
        return mutable_item

    def _on_sample(self, sample):
        """
        when a zenoh item appears, interact with self.mutable_items
        """
        logging.info("Received Zenoh message")
        if sample == self.last_sample:
            logging.debug("duplicate sample")
            return
        self.last_sample = sample

        mutable_item = self.sample_to_mutable_item(sample)
        with self._pending_lock:
            self._pending_items[mutable_item['robot_id']] = mutable_item

    def _drain_pending(self):
        with self._pending_lock:
            pending, self._pending_items = self._pending_items, {}
        for mutable_item in pending.values():
            self._handle_item(mutable_item)

    def _handle_item(self, mutable_item):
        robot_id = mutable_item['robot_id']
        seq = mutable_item['seq']

        if robot_id == self.robot_id:
            logging.info("received mutable item about own session; ignore")
            return

        if mutable_item['my_ip'] == self.my_ip:
            logging.info("received mutable item from own ip; ignore")
            return

        last_seq = self.max_seq_seen.get(robot_id, -1)
        if seq <= last_seq:
            logging.debug(f"ignoring stale seq {seq} for robot_id {robot_id}, have seq {last_seq}")
            return
        self.max_seq_seen[robot_id] = seq

        # highest seq we've seen. is this for a new robot or an update
        is_new = robot_id not in self.mutable_items
        self.mutable_items[robot_id] = mutable_item

        if is_new:
            logging.info(f"new mutable item for robot_id {robot_id}")
            # callback to orchestrator
            if self.on_new_item is not None:
                self.on_new_item(robot_id, mutable_item)
        else:
            logging.debug(f"updated mutable item for robot_id {robot_id}, seq {seq}"
            )
            if self.on_item_updated is not None:
                self.on_item_updated(robot_id, mutable_item)

