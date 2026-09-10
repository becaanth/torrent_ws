import math
import numpy as np
from collections import defaultdict

from agent import Agent, Submap ,Role, Status

import pdb

class World:
    def __init__(self, agents, link_radius, max_bandwidth, teach_hz=1.0, repeat_hz=1.0, dt=0.1):
        self.agents = {a.rid: a for a in agents}
        self.link_radius = link_radius
        self.max_bandwidth = max_bandwidth
        self.teach_hz = teach_hz
        self.teach_period = 1.0 / teach_hz
        self.repeat_hz = repeat_hz
        self.repeat_period = 1.0 / repeat_hz
        self.dt = dt
        self.t = 0.0
        self._next_teach_t = {
            a.rid: a.start_delay + self.teach_period for a in agents if a.role == Role.TEACH
        }
        # Start repeater timer after its individual delay window
        self._next_repeat_t = {
            a.rid: a.start_delay + self.repeat_period for a in agents if a.role == Role.REPEAT
        }

        self.history = []  # list of dicts, one per tick, for post-hoc plotting/debugging

    def run(self, n_steps, teach_path_fn=None):
        for _ in range(n_steps):
            teach_paths = teach_path_fn(self.t) if teach_path_fn else {}
            record = self.step(teach_paths)
            self.history.append(record)
        return self.history

    def step(self, teach_paths):
        """
        teach_paths: {rid -> (dx, dy)} consulted only on ticks a Teacher is
        actually due to teach. Repeat agents need no input -- they always
        try to advance along whichever session_id they were assigned at
        construction (agent.target_session), gated by their own cadence.
        """
        teach_paths = teach_paths or {}
        teachers = [a for a in self.agents.values() if a.role == Role.TEACH]
        repeaters = [a for a in self.agents.values() if a.role == Role.REPEAT]

        # teach
        for agent in teachers:
            if self.t + 1e-9 >= self._next_teach_t[agent.rid]:
                dx, dy = teach_paths.get(agent.rid, (0,0))
                agent.teach(dx, dy)
                self._next_teach_t[agent.rid] += self.teach_period

        # gossip
        for teacher in teachers:
            for repeater in repeaters:
                if self.t + 1e-9 >= repeater.start_delay:
                    self._sync_topology(teacher, repeater)

        # leech using peers own policy
        leech_requests = {}
        for agent in repeaters:
            if self.t + 1e-9 < agent.start_delay:
                continue  # Idle during delay window
            if agent.target_session not in agent.remote_maps:
                continue
            target = agent.select_leech_target(agent.target_session)
            if target is not None:
                leech_requests[agent.rid] = (agent.target_session, target)

        transfers = self._resolve_transfers(leech_requests)
        for (leecher_rid, session_id, piece_idx), num_bytes in transfers.items():
            self.agents[leecher_rid].receive_bytes(session_id, piece_idx, num_bytes)

        # repeat
        moved = {}
        for agent in repeaters:
            if self.t + 1e-9 < agent.start_delay:
                continue
            if self.t + 1e-9 >= self._next_repeat_t[agent.rid]:
                moved[agent.rid] = agent.topological_move(agent.target_session, movement=+1)
                self._next_repeat_t[agent.rid] += self.repeat_period

        self.t += self.dt
        return {'t': self.t, 'transfers': dict(transfers), 'moved': moved}


    def _sync_topology(self, teacher, repeater):
        """
        gossip if in range
        """
        if self.distance(teacher, repeater) > self.link_radius:
            return
        known = repeater.remote_maps.get(teacher.session_id)
        taught = teacher.local_maps
        if known is None:
            repeater.learn_topology(teacher.session_id, taught)
            return

        # sync new taught submaps
        for submap in taught[len(known):]:
            known.append(Submap(
                rid=teacher.rid, x=submap.x, y=submap.y, idx=submap.idx,
                state=Status.TOPOLOGY, size_bytes=repeater.piece_size,
            ))
            repeater.priorities[teacher.session_id] = np.append(
                repeater.priorities[teacher.session_id], 0
            )

    def distance(self, a,b):
        return math.hypot(a.x - b.x, a.y - b.y)

    def uplink_capacity(self, agent):
        """
        the agents own serving budget, following trs paper's U_p
        independant of distance, uplink budget/#(peers serving to)
        """
        return self.max_bandwidth * agent.radio_quality

    def channel_capacity(self, source, leecher):
        """
        physical ceiling for this specific pair regardless of budget
        """
        d = self.distance(source, leecher)
        if d > self.link_radius:
            return 0.0
        distance_factor = 1.0 - (d / self.link_radius)
        link_quality = min(source.radio_quality, leecher.radio_quality)
        return self.max_bandwidth * link_quality * distance_factor

    def _connected_sources(self, leecher, session_id, piece_idx):
        sources = []
        for rid, agent in self.agents.items():
            if rid == leecher.rid:
                continue
            if agent.has_full_data(session_id, piece_idx) and self.channel_capacity(agent, leecher) > 0:
                sources.append(agent)
        return sources

    def _resolve_transfers(self, leech_requests: dict[int, tuple[str, int, int]]):
        provider_queues = defaultdict(list)  # provider_id -> list of (leecher_id, session_id, piece_idx)
        demand_per_source = defaultdict(list) # source_rid -> [(leecher, session_id, piece_idx)]
        for leecher_rid, (session_id, piece_idx) in leech_requests.items():
            leecher = self.agents[leecher_rid]
            for source in self._connected_sources(leecher, session_id, piece_idx):
                demand_per_source[source.rid].append((leecher, session_id, piece_idx))

        transfers = defaultdict(float)
        for source_rid, servees in demand_per_source.items():
            source = self.agents[source_rid]
            n = len(servees)
            allocated_share = self.uplink_capacity(source) / n
            for leecher, session_id, piece_idx in servees:
                cap = self.channel_capacity(source, leecher)
                delivered_rate = min(allocated_share, cap)
                transfers[(leecher.rid, session_id, piece_idx)] += delivered_rate*self.dt
        return transfers