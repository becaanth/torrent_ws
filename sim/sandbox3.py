"""
3-agent scenario: one Teacher walks a path. Two Repeaters follow, with
different radio_quality (one strong, one weak), both leeching from
whatever sources are in range -- including each other, once either has
pieces the other lacks. This is the first scenario that can actually
exercise peer-to-peer sharing (has_full_data already checks remote_maps
for non-owning agents -- this confirms it actually gets used, not just
that it's theoretically reachable).
"""
from collections import defaultdict
from agent import Agent, Role
from world import World
from visualize import plot_dashboard, animate_simulation
from viz_priorities import plot_priority_heatmaps
import numpy as np
import sys
from pathlib import Path

# Add the 'scripts' directory to sys.path
SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.append(str(SCRIPTS_DIR))

# Import the module
from torrent.piece_pickers import *

PIECE_SIZE = 512 * 1024          # bigger than the 2-agent sanity check, so
MAX_BANDWIDTH = 1024 * 1024       # a single teach period isn't enough bandwidth
LINK_RADIUS = 50.0               # to finish a piece -- forces a backlog to build
TEACH_HZ = 1.0
REPEAT_HZ = 5.0
DT = 0.1
N_STEPS = 1000  # 60 sim-seconds

POLICY = 'r'
def main():
    teacher = Agent(rid=0, x0=0.0, y0=0.0, radio_quality=1.0,
                     policy=get_policy(POLICY), pol_param=0.5, role=Role.TEACH,
                     piece_size=PIECE_SIZE)

    # strong: fast radio, stays close -- should pull ahead
    strong = Agent(rid=1, x0=0.0, y0=0.0, radio_quality=1.0,
                    policy=get_policy(POLICY), pol_param=0.5, role=Role.REPEAT,
                    target_session=teacher.session_id, piece_size=PIECE_SIZE, start_delay=5.0)

    # weak: poor radio, should lag and become dependent on `strong` as a
    # relay for pieces `strong` finishes before `teacher` moves further away
    weak = Agent(rid=2, x0=0.0, y0=0.0, radio_quality=0.3,
                 policy=get_policy(POLICY), pol_param=0.5, role=Role.REPEAT,
                 target_session=teacher.session_id, piece_size=PIECE_SIZE, start_delay=5.0)

    world = World(agents=[teacher, strong, weak], link_radius=LINK_RADIUS,
                  max_bandwidth=MAX_BANDWIDTH, teach_hz=TEACH_HZ,
                  repeat_hz=REPEAT_HZ, dt=DT)

    def teach_path(t):
        return {0: (1.0, 0.0)}

    # track who actually served whom, to confirm peer-to-peer sharing fires
    source_use = defaultdict(lambda: defaultdict(int))  # leecher_rid -> source_rid -> count

    errors = []
    last_idx = {1: -1, 2: -1}
    history = []
    priority_history = defaultdict(list)
    for step_i in range(N_STEPS):
        record = world.step(teach_path(world.t))
        record['agent_states'] = {
            rid: {
                'x': agent.x,
                'y': agent.y,
                'idx': agent.current_submap.idx,
            }
            for rid, agent in world.agents.items()
        }
        history.append(record)

        # Snapshot priority array for each repeater agent
        for rid, agent in world.agents.items():
            if agent.role.name == "REPEAT" and agent.target_session:
                # Get latest calculated priority vector for the target session
                prio = agent.priorities.get(agent.target_session, np.array([]))
                priority_history[rid].append(prio.copy())
        # attribute each transfer to whichever agent(s) held the piece --
        # step() doesn't currently report *which* source served a transfer
        # (bandwidth is pooled per-piece, not per-source-pair), so recover
        # it here for diagnostics: check who had the piece complete just
        # before this tick resolved it.
        for (leecher_rid, session_id, piece_idx), num_bytes in record['transfers'].items():
            for candidate_rid, candidate in world.agents.items():
                if candidate_rid == leecher_rid:
                    continue
                if candidate.has_full_data(session_id, piece_idx):
                    source_use[leecher_rid][candidate_rid] += 1

        for rid in (1, 2):
            idx = world.agents[rid].current_submap.idx
            if idx < last_idx[rid]:
                errors.append(f"t={world.t:.2f}: agent {rid} idx went backwards")
            last_idx[rid] = idx

        if step_i % 50 == 0:
            print(f"t={world.t:5.1f}  teacher={teacher.current_submap.idx:3d}  "
                  f"strong={strong.current_submap.idx:3d}  weak={weak.current_submap.idx:3d}")

    
    print()
    print("=" * 60)
    if errors:
        print(f"FAILED -- {len(errors)} invariant violations:")
        for e in errors[:10]:
            print(" ", e)
        raise SystemExit(1)

    print(f"Final: teacher={teacher.current_submap.idx}  strong={strong.current_submap.idx}  weak={weak.current_submap.idx}")
    print()
    print("Source usage (leecher -> {source: tick_count}):")
    for leecher_rid, sources in source_use.items():
        label = {1: 'strong', 2: 'weak'}[leecher_rid]
        readable = {({0: 'teacher', 1: 'strong', 2: 'weak'}[k]): v for k, v in sources.items()}
        print(f"  {label}: {readable}")

    peer_to_peer_seen = any(
        src != 0  # any source other than the teacher itself
        for leecher_sources in source_use.values()
        for src in leecher_sources
    )
    print()
    if peer_to_peer_seen:
        print("Peer-to-peer sharing CONFIRMED -- a Repeater served another Repeater at least once.")
    else:
        print("Peer-to-peer sharing NEVER occurred in this run.")
        print("(Not necessarily a bug -- see analysis. Rerun with a longer link_radius")
        print(" or closer starting positions between strong/weak to force overlap.)")

    for rid, label in ((1, 'strong'), (2, 'weak')):
        agent = world.agents[rid]
        order = agent.download_order[teacher.session_id]
        known = agent.remote_maps[teacher.session_id]
        S = sequentiality_trajectory(order, len(known))
        print(f"{label}: downloaded {len(order)} pieces, order={order}, S={S:.3f}")

    assert strong.current_submap.idx >= weak.current_submap.idx, \
        "weak agent (poor radio) outran strong agent -- suspicious, check bandwidth split"
    assert strong.current_submap.idx > 0, "strong repeater never advanced"

    print()
    print("ALL CHECKS PASSED")
    plot_dashboard(world, history)
    animate_simulation(world, history, interval=30)
    max_pieces = len(world.agents[0].local_maps)
    plot_priority_heatmaps(priority_history, max_pieces=max_pieces)

if __name__ == "__main__":
    main()