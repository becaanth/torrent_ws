import matplotlib.pyplot as plt
import matplotlib.animation as animation
from collections import defaultdict

def plot_dashboard(world, history, save_path="simulation_dashboard.png"):
    """
    Plots physical movement, submap advancement, and download throughput over time.
    """
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    
    colors = {0: 'crimson', 1: 'royalblue', 2: 'darkorange'}
    labels = {0: 'Teacher', 1: 'Strong Repeater', 2: 'Weak Repeater'}
    times = [rec['t'] for rec in history]

    # --- Panel 1: Physical Position (X Axis) over Time ---
    ax1 = axes[0]
    ax1.set_title("Physical X Position over Time")
    for rid in (0, 1, 2):
        xs = [rec['agent_states'][rid]['x'] for rec in history]
        ax1.plot(times, xs, label=labels[rid], color=colors[rid], lw=2)
    ax1.set_xlabel("Time (s)")
    ax1.set_ylabel("X Position (m)")
    ax1.legend()
    ax1.grid(True, linestyle='--', alpha=0.6)

    # --- Panel 2: Submap Index over Time ---
    ax2 = axes[1]
    ax2.set_title("Current Submap Index over Time")
    for rid in (0, 1, 2):
        idxs = [rec['agent_states'][rid]['idx'] for rec in history]
        ax2.plot(times, idxs, label=labels[rid], color=colors[rid], lw=2)
    ax2.set_xlabel("Time (s)")
    ax2.set_ylabel("Submap Index")
    ax2.legend()
    ax2.grid(True, linestyle='--', alpha=0.6)

    # --- Panel 3: Active Transfer Rates ---
    ax3 = axes[2]
    ax3.set_title("Download Throughput")
    leecher_rates = defaultdict(list)
    for rec in history:
        tick_bytes = defaultdict(float)
        for (leecher_rid, _, _), nbytes in rec['transfers'].items():
            tick_bytes[leecher_rid] += nbytes
        
        for rid in (1, 2):
            rate_kbps = (tick_bytes[rid] / world.dt) / 1024  # KB/s
            leecher_rates[rid].append(rate_kbps)

    for rid in (1, 2):
        ax3.plot(times, leecher_rates[rid], label=f"{labels[rid]} Leech Rate", color=colors[rid], alpha=0.8)

    ax3.set_xlabel("Time (s)")
    ax3.set_ylabel("Throughput (KB/s)")
    ax3.legend()
    ax3.grid(True, linestyle='--', alpha=0.6)

    plt.tight_layout()
    plt.savefig(save_path, dpi=300)
    print(f"Dashboard saved to {save_path}")
    plt.show()


def animate_simulation(world, history, interval=50):
    """
    Animated 2D view showing live agent movement, radio ranges,
    and active P2P transfers per time step.
    """
    fig, ax = plt.subplots(figsize=(8, 8))
    ax.set_title("Swarm P2P Simulation")
    ax.set_xlabel("X (m)")
    ax.set_ylabel("Y (m)")
    ax.set_aspect('equal')
    ax.grid(True, linestyle='--', alpha=0.5)

    colors = {0: 'crimson', 1: 'royalblue', 2: 'darkorange'}
    markers = {0: '*', 1: 'o', 2: 's'}

    # Set bounds based on maximum physical distance reached
    max_x = max(rec['agent_states'][0]['x'] for rec in history) + 20
    ax.set_xlim(-10, max_x)
    ax.set_ylim(-10, 30)

    agent_dots = {}
    radius_circles = {}
    transfer_lines = []

    # Initial plot elements
    first_state = history[0]['agent_states']
    for rid, state in first_state.items():
        (dot,) = ax.plot([state['x']], [state['y']], marker=markers[rid], color=colors[rid], ms=10, zorder=5, label=f"Agent {rid}")
        circle = plt.Circle((state['x'], state['y']), world.link_radius, color=colors[rid], fill=False, linestyle=':', alpha=0.3)
        ax.add_patch(circle)
        
        agent_dots[rid] = dot
        radius_circles[rid] = circle

    time_text = ax.text(0.02, 0.95, '', transform=ax.transAxes)
    ax.legend(loc='lower right')

    def update(frame_idx):
        nonlocal transfer_lines
        for line in transfer_lines:
            line.remove()
        transfer_lines.clear()

        record = history[frame_idx]
        states = record['agent_states']
        time_text.set_text(f"t = {record['t']:.1f}s")

        # Update position of each agent from history snapshot
        for rid, state in states.items():
            agent_dots[rid].set_data([state['x']], [state['y']])
            radius_circles[rid].center = (state['x'], state['y'])

        # Draw line between leecher and source during active transfers
        for (leecher_rid, session_id, piece_idx), nbytes in record['transfers'].items():
            if nbytes <= 0:
                continue
            leecher_pos = (states[leecher_rid]['x'], states[leecher_rid]['y'])
            
            # Identify active source candidate
            for src_rid, src_state in states.items():
                if src_rid != leecher_rid and src_state['idx'] >= piece_idx:
                    line_color = 'limegreen' if src_rid == 0 else 'purple'
                    (line,) = ax.plot(
                        [src_state['x'], leecher_pos[0]],
                        [src_state['y'], leecher_pos[1]],
                        color=line_color, lw=1.5, linestyle='-', alpha=0.8, zorder=4
                    )
                    transfer_lines.append(line)

        return list(agent_dots.values()) + list(radius_circles.values()) + [time_text] + transfer_lines

    ani = animation.FuncAnimation(fig, update, frames=len(history), interval=interval, blit=False)
    plt.show()