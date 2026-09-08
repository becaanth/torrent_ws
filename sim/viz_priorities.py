import numpy as np
import matplotlib.pyplot as plt

def plot_priority_heatmaps(priority_history, max_pieces, save_path="priority_heatmaps.png"):
    """
    Renders 2D heatmaps of piece priorities (Y-axis: simulation step, X-axis: piece index)
    for each repeater agent over time.
    """
    repeaters = list(priority_history.keys())
    num_agents = len(repeaters)
    if num_agents == 0:
        print("No repeater priority history found.")
        return

    fig, axes = plt.subplots(num_agents, 1, figsize=(10, 3 * num_agents), sharex=True)
    if num_agents == 1:
        axes = [axes]

    for ax, rid in zip(axes, repeaters):
        p_hist = priority_history[rid]
        steps = len(p_hist)
        
        # Build matrix padded to max_pieces
        res = np.zeros((steps, max_pieces))
        for step_i, prio_arr in enumerate(p_hist):
            if prio_arr is not None and len(prio_arr) > 0:
                length = min(len(prio_arr), max_pieces)
                res[step_i, :length] = prio_arr[:length]

        im = ax.imshow(res, aspect='auto', origin='upper', cmap='viridis')
        ax.set_title(f"Agent {rid} Piece Priorities Over Time")
        ax.set_ylabel("Simulation Step")
        fig.colorbar(im, ax=ax, label="Priority Level")

    axes[-1].set_xlabel("Piece Index")
    plt.tight_layout()
    plt.savefig(save_path, dpi=300)
    print(f"Priority heatmaps saved to {save_path}")
    plt.show()