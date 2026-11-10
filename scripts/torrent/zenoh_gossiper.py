import zenoh
import msgpack
import logging
import time
from threading import Lock

from torrent.torrent_utils import unpack_device, DiffLog

try:
    from utils import evt
except ImportError:
    def evt(kind, **f): logging.info(f"EVT {kind} {f}")

logger = logging.getLogger(__name__)

class ZenohGossiper:
    """
    Handle mutable item Zenoh pub/sub between agents in the swarm
    This is an implementation of a discovery for mutable torrents on MANETs
    A mutable item contains:
    - the robot id of the source of the pieces
    - the infohash of the torrent session
    - a (monotonically) increasing sequence number for version control
    - an epoch (the seeder's start time, ns): seqs restart at a seeder restart, so versions
      are ordered by (epoch, seq)
    - on the original publish only: diff = D[seq], the metadata diff from seq-1 (see torrent_utils)

    Consensus is enforced because
    1) only the source of the pieces can increment the sequence
    2) T&R maps are append-only, so stale hashes can still seed useful info among disconnected peers

    Metadata diffs: every diff seen (published, gossiped or fetched) goes into self.diff_log, and a
    queryable on ttr/diffs/{robot}/{epoch}?from=a;to=b answers with whichever of D[a..b] this agent
    holds, so peers that missed some seqs can catch up from any neighbour, not only the seeder.
    """
    def __init__(self, params : dict, robot_id : int, poll_hz : float = 1.0,
        on_new_item=None, on_item_updated=None, zenoh_cfg=None, my_ip=None):
        # setup
        self.container = params['container']
        self.router = params['router']
        self.robot_id = robot_id

        # zenoh objects (zenoh_cfg/my_ip override the device config: offline tests over loopback)
        if zenoh_cfg is None:
            self.my_ip, z_cfg = unpack_device(params, self.robot_id, zenoh_port=5203)
        else:
            self.my_ip, z_cfg = my_ip, zenoh_cfg
        z_cfg.insert_json5("open/return_conditions/connect_scouted", "false")
        self.z_ses = zenoh.open(z_cfg)
        self.sub = self.z_ses.declare_subscriber("mutable_items/**", self._on_sample)
        self.diff_log = DiffLog()
        self.qbl = self.z_ses.declare_queryable("ttr/diffs/**", self._on_query)
        logging.info("Zenoh session init")

        self.last_sample = None
        self._pending_lock = Lock()
        self._pending_items = {} # robot_id -> latest not processed mi

        self.mutable_items = {} # robot_id -> latest known mi
        self._items_lock = Lock()
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
        diff = item.get('diff')
        if diff is not None:
            self.diff_log.store(item['robot_id'], item.get('epoch', 0), item['seq'], diff)
        stored = {k: v for k, v in item.items() if k != 'diff'}   # rebroadcasts don't repeat the diff
        with self._items_lock:
            self.mutable_items[item['robot_id']] = stored
            self.max_seq_seen[item['robot_id']] = (item.get('epoch', 0), item['seq'])
        self._put(item, kind="publish")

    def _put(self, mutable_item : dict, kind: str = "forward"):
        payload = msgpack.packb(mutable_item, use_bin_type=True)
        ih = mutable_item.get('infohash')
        diff = mutable_item.get('diff')
        evt("gossip_tx", origin=mutable_item['robot_id'], seq=mutable_item['seq'], kind=kind,
        ih=bytes(ih).hex() if isinstance(ih, (bytes, bytearray)) else None, bytes=len(payload),
        diff_bytes=len(diff) if diff is not None else 0)
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
        with self._items_lock:
            items_snapshot = list(self.mutable_items.values())

        for mutable_item in items_snapshot:
            self._forward_gossip(mutable_item)

    def sample_to_mutable_item(self, sample):
        # Update mutable item
        zenoh_item = msgpack.unpackb(bytes(sample.payload), raw=False)
        mutable_item = {
            # 'pubkey' : zenoh_item.get('pubkey'),
            'robot_id' : zenoh_item.get('robot_id'),
            'seq' : zenoh_item.get('seq'),
            'epoch' : zenoh_item.get('epoch', 0),    # 0: robot on a seeder from before epochs
            'infohash' : zenoh_item.get('infohash'),
            'my_ip' : zenoh_item.get('my_ip'),
            'diff' : zenoh_item.get('diff'),
        }
        return mutable_item

    def _on_sample(self, sample):
        """
        when a zenoh item appears, interact with self.mutable_items
        """
        logging.info("Received Zenoh message")
        payload = bytes(sample.payload)
        mi = msgpack.unpackb(payload, raw=False)
        ih = mi.get('infohash')
        diff = mi.get('diff')
        evt("gossip_rx", origin=mi.get('robot_id'), seq=mi.get('seq'), via_ip=mi.get('my_ip'),
            ih=bytes(ih).hex() if isinstance(ih, (bytes, bytearray)) else None, bytes=len(payload),
            diff_bytes=len(diff) if diff is not None else 0, rx=self.robot_id)

        if sample == self.last_sample:
            logging.debug("duplicate sample")
            return
        self.last_sample = sample

        mutable_item = self.sample_to_mutable_item(sample)
        # keep every diff that passes by, before any staleness check: a peer that has already
        # moved past this seq can still serve it to one that hasn't
        if diff is not None and mutable_item['robot_id'] != self.robot_id:
            self.diff_log.store(mutable_item['robot_id'], mutable_item['epoch'], mutable_item['seq'], diff)
        mutable_item.pop('diff', None)
        with self._pending_lock:
            self._pending_items[mutable_item['robot_id']] = mutable_item

    def _on_query(self, query):
        """
        ttr/diffs/{robot}/{epoch}[/origin]?from=a;to=b -> msgpack {seq: blob} of the ones held; no reply if none
        with /origin only the robot whose diffs they are answers (it always holds its whole epoch)
        """
        try:
            parts = str(query.key_expr).split("/")
            robot, epoch = int(parts[2]), int(parts[3])
            a, b = int(query.parameters.get("from")), int(query.parameters.get("to"))
        except Exception as e:
            logging.debug(f"bad diff query {query.selector}: {e}")
            return
        if len(parts) > 4 and parts[4] == "origin" and robot != int(self.robot_id):
            return
        have = self.diff_log.get_range(robot, epoch, a, b)
        if have:
            query.reply(query.key_expr, msgpack.packb(have, use_bin_type=True))

    def fetch_diffs(self, robot, epoch, a, b, timeout=2.0):
        """
        D[a..b] for robot/epoch: local log first, then whichever neighbours hold the rest.
        Returns ({seq: blob}, bytes received over the network). The map may be incomplete.
        Blocks for up to `timeout` s; never call it while holding a lock a Zenoh callback needs.
        """
        got = self.diff_log.get_range(robot, epoch, a, b)
        missing = [s for s in range(a, b + 1) if s not in got]
        wire = 0
        # ask the origin first (one reply with everything, if it's reachable), then everyone for
        # whatever is still missing: asking everyone at once would bring a copy from every holder
        for scope in ("/origin", ""):
            todo = [s for s in missing if s not in got]
            if not todo:
                break
            sel = f"ttr/diffs/{int(robot)}/{int(epoch)}{scope}?from={todo[0]};to={todo[-1]}"
            try:
                # holders reply on the same key: the default (auto) consolidation would keep only
                # one of them and drop the rest of a range split across agents
                for reply in self.z_ses.get(sel, timeout=timeout, target=zenoh.QueryTarget.ALL,
                                            consolidation=zenoh.ConsolidationMode.NONE):
                    if reply.ok is None:
                        continue
                    raw = reply.ok.payload.to_bytes()
                    wire += len(raw)
                    for s, blob in msgpack.unpackb(raw, raw=False, strict_map_key=False).items():
                        if a <= int(s) <= b:
                            got.setdefault(int(s), bytes(blob))
            except Exception as e:
                logging.warning(f"diff query {sel} failed: {e}")
        for s in missing:
            if s in got:
                self.diff_log.store(robot, epoch, s, got[s])   # now we can serve them too
        evt("diff_fetch", origin=int(robot), epoch=int(epoch), lo=a, hi=b,
            local=(b - a + 1) - len(missing), fetched=sum(1 for s in missing if s in got),
            missing=sum(1 for s in missing if s not in got), bytes=wire)
        return got, wire

    def _drain_pending(self):
        with self._pending_lock:
            pending, self._pending_items = self._pending_items, {}
        for mutable_item in pending.values():
            self._handle_item(mutable_item)

    def _handle_item(self, mutable_item):
        robot_id = mutable_item['robot_id']
        seq = mutable_item['seq']
        version = (mutable_item.get('epoch', 0), seq)

        if robot_id == self.robot_id:
            logging.info("received mutable item about own session; ignore")
            return

        if mutable_item['my_ip'] == self.my_ip:
            logging.info("received mutable item from own ip; ignore")
            return

        with self._items_lock:
            last = self.max_seq_seen.get(robot_id, (-1, -1))
            if version <= last:
                logging.debug(f"ignoring stale (epoch, seq) {version} for robot_id {robot_id}, have {last}")
                return
            self.max_seq_seen[robot_id] = version
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

