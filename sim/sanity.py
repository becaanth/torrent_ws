"""
Minimal sanity check: one TEACH agent walks a straight line, creating a new
submap once per second. One REPEAT agent, starting at the same position,
tries to follow -- leeching from the Teacher every tick, attempting to
advance once per second.

Goal: confirm gossip sync, bandwidth-limited leeching, and repeat-gating
all work end-to-end and the Repeater's progress makes physical sense
(bounded above by teach cadence, degraded by distance/piece size/bandwidth).
"""
import sys
from agent import Agent, Role, Status
from world import World

import sys
from pathlib import Path

# Add the 'scripts' directory to sys.path
SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.append(str(SCRIPTS_DIR))

# Import the module
from torrent.piece_pickers import *

PIECE_SIZE = 256 * 1024       # 256 KiB -- small, so a couple agents can finish fast for the sanity check
MAX_BANDWIDTH = 1 * 1024 * 1024  # 1 MiB/s peak uplink at quality=1, distance=0
LINK_RADIUS = 50.0
TEACH_HZ = 1.0
REPEAT_HZ = 1.0
DT = 0.1
N_STEPS = 300  # 30 sim-seconds


def main():
    teacher = Agent(
        rid=0, x0=0.0, y0=0.0, radio_quality=1.0,
        policy=get_policy('rarest-random'), role=Role.TEACH,
        piece_size=PIECE_SIZE,
    )
    repeater = Agent(
        rid=1, x0=0.0, y0=0.0, radio_quality=1.0,
        policy=get_policy('rarest-random'), role=Role.REPEAT,
        target_session=teacher.session_id, piece_size=PIECE_SIZE,
    )

    world = World(
        agents=[teacher, repeater],
        link_radius=LINK_RADIUS,
        max_bandwidth=MAX_BANDWIDTH,
        teach_hz=TEACH_HZ, repeat_hz=REPEAT_HZ, dt=DT,
    )

    def teach_path(t):
        return {0: (1.0, 0.0)}  # teacher walks +1 unit in x per teach tick

    errors = []
    last_repeat_idx = -1

    for step_i in range(N_STEPS):
        teach_paths = teach_path(world.t)
        record = world.step(teach_paths)

        # sanity assertions, checked every tick
        rep_idx = repeater.current_submap.idx
        if rep_idx < last_repeat_idx:
            errors.append(f"t={world.t:.2f}: repeat idx went backwards ({last_repeat_idx} -> {rep_idx})")
        if rep_idx > teacher.current_submap.idx:
            errors.append(f"t={world.t:.2f}: repeater ({rep_idx}) ahead of teacher ({teacher.current_submap.idx})")
        last_repeat_idx = rep_idx

        if step_i % 10 == 0:
            known = repeater.remote_maps.get(teacher.session_id, [])
            n_complete = sum(1 for p in known if p.is_complete)
            print(f"t={world.t:5.1f}  teacher_idx={teacher.current_submap.idx:3d}  "
                  f"repeater_pos={rep_idx:3d}  known_pieces={len(known):3d}  "
                  f"complete={n_complete:3d}")

    print()
    print("=" * 60)
    if errors:
        print(f"FAILED -- {len(errors)} invariant violations:")
        for e in errors[:10]:
            print(" ", e)
        sys.exit(1)

    # basic end-state checks
    teacher_final_idx = teacher.current_submap.idx
    repeater_final_idx = repeater.current_submap.idx
    print(f"Teacher created {teacher_final_idx} new submaps (+ origin)")
    print(f"Repeater advanced to idx {repeater_final_idx}")

    assert teacher_final_idx > 0, "teacher never taught anything -- cadence gating broken"
    assert repeater_final_idx > 0, "repeater never advanced -- leech/repeat pipeline broken"
    assert repeater_final_idx <= teacher_final_idx, "repeater outran teacher -- impossible"

    known = repeater.remote_maps[teacher.session_id]
    order = repeater.download_order[teacher.session_id]
    S = sequentiality_trajectory(order, len(known))
    print(f"Repeater download order: {order}")
    print(f"Sequentiality (trajectory-based): {S:.3f}")

    # with only one source and a follow-the-leader path, downloads should be
    # essentially in order (piece 0 exists first, is nearest in time, etc.)
    assert S > 0.8, f"expected near-sequential download with a single lagging source, got S={S:.3f}"

    print()
    print("ALL SANITY CHECKS PASSED")


if __name__ == "__main__":
    main()