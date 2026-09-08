"""Second sanity check: repeater permanently out of link_radius should
never learn topology or advance at all -- confirms range-gating fails
closed rather than silently leaking progress."""
from agent import Agent, Role
from world import World
import sys
from pathlib import Path

# Add the 'scripts' directory to sys.path
SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.append(str(SCRIPTS_DIR))

# Import the module
from torrent.piece_pickers import get_policy

PIECE_SIZE = 512 * 1024
MAX_BANDWIDTH = 1 * 1024 * 1024
LINK_RADIUS = 50.0
N_STEPS = 600

teacher = Agent(rid=0, x0=0.0, y0=0.0, radio_quality=1.0,
                 policy=get_policy('rarest-random'), role=Role.TEACH, piece_size=PIECE_SIZE)
repeater = Agent(rid=1, x0=1000.0, y0=0.0, radio_quality=1.0,  # far outside link_radius
                  policy=get_policy('rarest-random'), role=Role.REPEAT,
                  target_session=teacher.session_id, piece_size=PIECE_SIZE)

world = World(agents=[teacher, repeater], link_radius=LINK_RADIUS,
              max_bandwidth=MAX_BANDWIDTH, teach_hz=1.0, repeat_hz=1.0, dt=0.1)

for _ in range(N_STEPS):
    world.step({0: (1.0, 0.0)})

known = repeater.remote_maps.get(teacher.session_id, [])
print(f"Teacher taught {teacher.current_submap.idx} submaps")
print(f"Out-of-range repeater knows about {len(known)} pieces (expect 0)")
print(f"Out-of-range repeater position idx={repeater.current_submap.idx} (expect 0)")

assert len(known) == 0, "repeater learned topology despite being out of range -- gossip gating broken"
assert repeater.current_submap.idx == 0, "repeater advanced despite never syncing -- repeat gating broken"
print("\nOUT-OF-RANGE CHECK PASSED")